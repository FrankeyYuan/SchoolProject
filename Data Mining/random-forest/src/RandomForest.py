import random  # for creating bootstrap and randomly select attribute

from DecisionTree import DecisionTree

__author__ = "Yufeng Yuan"
__email__  = "Yufeng_Yuan@student.uml.edu"

class RandomForest(object):
    """
    Class of the Random Forest
    """
    def __init__(self, tree_num):
        self.tree_num = int(tree_num)
        self.forest = []

    def train(self, records, attributes):
        """
        This function will train the random forest, the basic idea of training a
        Random Forest is as follows:
        1. Draw n bootstrap samples using bootstrap() function
        2. For each of the bootstrap samples, grow a tree with a subset of
            original attributes, which is of size m (m << # of total attributes)
        """
        m = 10 # cause in the main function, user cannot set the the number of attributes, so I set the default as 10

        for i in range(self.tree_num):
            # generate a bootstrap sample
            bs_records = self.bootstrap(records)
            # randomly select m attbutes for building decision tree
            attr_subset = sorted(random.sample(attributes, m))
            # Build decision tree with bootstrap sample and random m attributes
            tree = DecisionTree()
            tree.train(bs_records, attr_subset)
            self.forest.append(tree)
        pass

    def predict(self, sample):
        """
        The predict function predicts the label for new data by aggregating the
        predictions of each tree.

        This function should return the predicted label
        """
        # predict the test set by using each decision tree
        p_results = []
        for dtree in self.forest:
            p_results.append(dtree.predict(sample))
        # vote the majority result
        if p_results.count('p') > p_results.count('e'):
            return 'p'
        return 'e'

    def bootstrap(self, records):
        """
        This function bootstrap will return a set of records, which has the same
        size with the original records but with replacement.
        """
        bootstrap_sample = [random.choice(records) for _ in records]
        return bootstrap_sample

