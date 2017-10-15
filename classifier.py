from sklearn.preprocessing import normalize
from sklearn.feature_selection import SelectFromModel
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import TimeSeriesSplit
from sklearn.model_selection import GridSearchCV
from sklearn.svm import SVC
from get_collected_data import get_coin_data_all
import matplotlib.pyplot as plt
import pickle
import time


CLF_PATH = 'models/classifier.p'
FEATURE_SEL_PATH = 'models/feature_sel.p'


class CryptoClassifier():
    def __init__(self, data=None, train=False, grid_search=False):
        # initially, all models are None
        self.feature_selector = None
        self.clf = None

        if train:
            if data is not None:
                self.train_data, self.target_data = self._prepare_data(data)
                self.train(grid_search)
            else:
                print('Train data must be provided!')
        else:
            self.load()

    @staticmethod
    def _prepare_data(data):
        data = data[['volume', 'close']]
        data['returns'] = data['close'].pct_change()
        data['target'] = data['returns'].shift(-1).map(lambda x: int(x > 0))

        data = CryptoClassifier.feature_generation(data)
        features = data.drop('target', axis=1).values
        target = data['target'].values
        return features, target

    @staticmethod
    def feature_generation(data):
        # prepare rolling ewm
        for momentum in range(10, 200, 20):
            column = 'ewm_{}'.format(momentum)
            data[column] = data['returns'].ewm(span=momentum, adjust=False).mean()
            # column1 = 'momentum_{}'.format(momentum)
            # data[column1] = data['returns'].rolling(momentum).mean()

        # prepare lagged returns
        for delay in range(1, 20):
            column = 'delay_return_{}'.format(delay)
            data[column] = data['returns'].shift(delay)

        # prepare volume data
        data['volume'] = data['volume'].map(normalize)
        data.dropna(inplace=True)
        return data

    def feature_selection(self):
        clf = RandomForestClassifier(n_estimators=20, n_jobs=-1)
        clf = clf.fit(self.train_data, self.target_data)
        print('Feature selection')
        print('Accuracy:', accuracy_score(self.target_data, clf.predict(self.train_data)))
        print('Feature importance')
        print(clf.feature_importances_)
        self.feature_selector = SelectFromModel(clf, prefit=True)

    def train(self, grid_search):
        print('Training started...')
        start_time = time.time()
        self.feature_selection()
        print('Feature size before selection:', self.train_data.shape)
        self.train_data = self.feature_selector.transform(self.train_data)
        print('Feature size after selection:', self.train_data.shape)

        if grid_search:
            parameters = {'C': [1, 5, 10],
                          'kernel': ['linear', 'rgf', 'poly']}
            self.clf = GridSearchCV(SVC(), parameters, n_jobs=-1)
        else:
            # self.clf = SVC(C=10)
            self.clf = RandomForestClassifier(n_estimators=100, n_jobs=-1)
        tscv = TimeSeriesSplit(n_splits=5)
        fold_nr = 1
        for train_index, test_index in tscv.split(self.train_data):
            print('------------------------------------')
            print('Training on fold:', fold_nr)
            print('Train data size:', len(train_index))
            print('Test data size:', len(test_index))
            print()
            fold_nr += 1
            features_train = self.train_data[train_index]
            target_train = self.target_data[train_index]
            self.clf.fit(features_train, target_train)

            features_test = self.train_data[test_index]
            target_test = self.target_data[test_index]
            print('Accuracy:', accuracy_score(target_test, self.clf.predict(features_test)))
            print('------------------------------------')
            print()

        # now train on whole data
        self.clf.fit(self.train_data, self.target_data)
        print('Accuracy on entire train data:', accuracy_score(self.target_data, self.clf.predict(self.train_data)))
        print('Training took {} seconds'.format(time.time() - start_time))

        # save model
        pickle.dump(self.clf, open(CLF_PATH, 'wb'))
        pickle.dump(self.feature_selector, open(FEATURE_SEL_PATH, 'wb'))

    def load(self):
        try:
            self.clf = pickle.load(open(CLF_PATH, 'rb'))
            self.feature_selector = pickle.load(open(FEATURE_SEL_PATH, 'rb'))
        except:
            'Training must be performed before loading!'

    def check_prediction(self, data):
        features, target = self._prepare_data(data)
        features = self.feature_selector.transform(features)
        prediction = self.clf.predict(features)

        print(classification_report(target, prediction))


if __name__ == '__main__':
    data = get_coin_data_all('USDT-ETH')
    # INFO: another approach would be to read data from csv
    # data = pd.read_csv('data\eth.csv')

    data.plot(x='time')
    plt.show()
    clf = CryptoClassifier(data, train=True)
    clf1 = CryptoClassifier()
    clf1.check_prediction(data)
