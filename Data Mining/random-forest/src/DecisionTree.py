from math import log # for calucate the entropy

__author__ = "Yufeng Yuan"
__email__  = "Yufeng_Yuan@student.uml.edu"


class TreeNode(object):

    def __init__(self, isLeaf=False):
        self.isLeaf = isLeaf
        self.attribute = None
        self.left = None      # left child
        self.right = None     # right child
        # self.depth = None     # tree depth, not use because of the given model args with out max_depth
        self.val = None       # tree value

    def predict(self, sample):
        """
        This function predicts the label of given sample
        """
        if self.left is None and self.right is None:
            return self.val
        sample_value = sample['attributes'][self.attribute]
        if sample_value == self.val:
            return self.left.predict(sample)
        else:
            return self.right.predict(sample)


class DecisionTree(object):
    """
    Class of the Decision Tree
    """
    def __init__(self):
        self.root = None

    def train(self, records, attributes):
        """
        This function trains the model with training records "records" and
        attribute set "attributes", the format of the data is as follows:
            records: training records, each record contains following fields:
                label - the lable of this record
                attributes - a list of attribute values
            attributes: a list of attribute indices that you can use for
                        building the tree
        Typical data will look like:
            records: [
                        {
                            "label":"p",
                            "attributes":['p','x','y',...]
                        },
                        {
                            "label":"e",
                            "attributes":['b','y','y',...]
                        },
                        ...]
            attributes: [0, 2, 5, 7,...]
        """
        self.root = self.tree_growth(records, attributes)

    def predict(self, sample):

        """
        This function predict the label for new sample by calling the predict
        function of the root node
        """
        return self.root.predict(sample)

    def stopping_cond(self, records, attributes):
        """
        The stopping_cond() function is used to terminate the tree-growing
        process by testing whether all the records have either the same class
        label or the same attribute values.

        This function should return True/False to indicate whether the stopping
        criterion is met
        """

        # all records have same class
        class_list = [record['label'] for record in records]
        if class_list.count(class_list[0]) == len(class_list):
            return True

        # all records have same attribute values
        for record_index in range(1,len(records)):
            for attribute_index in attributes:
                if len(set([record['attributes'][attribute_index] for record in records])) != 1:
                    return False
        return True

    def classify(self, records):
        """
        This function determines the class label to be assigned to a leaf node.
        In most cases, the leaf node is assigned to the class that has the
        majority number of training records

        This function should return a label that is assigned to the node
        """
        label_list = [record['label'] for record in records]
        label_set = set(label_list)
        majority = ""
        # all records have same class, return this class
        if len(label_set) == 0:
            majority = label_list[0]
        else:
        # all records have same attribute values
            max_label = 0
            for label in label_set:
                if label_list.count(label) > max_label:
                    max_label = label_list.count(label)
                    majority = label

        return majority

    def find_best_split(self, records, attributes):
        """
        The find_best_split() function determines which attribute should be
        selected as the test condition for splitting the trainig records.

        This function should return multiple information:
            attribute selected for splitting
            threshhold value for splitting
            left subset
            right subset
        """
        whole_entropy = self.entropy(records)
        max_info_gain = 0
        attribute_selected = 0
        threshold_value = ""
        for i in attributes:
            value_list = set([example['attributes'][i] for example in records])

            for attributeValue in value_list:
                ret_records = []
                ret_records_number = 0
                non_ret_records = []
                non_ret_records_number = 0
                for line in records:
                    if line['attributes'][i] == attributeValue:
                        ret_records.append(line)
                        ret_records_number += 1
                    else:
                        non_ret_records.append(line)
                        non_ret_records_number += 1

                info_gain = whole_entropy - float(ret_records_number)/float(len(records)) * self.entropy(ret_records) \
                                          - float(non_ret_records_number)/float(len(records)) * self.entropy(non_ret_records)
                if info_gain > max_info_gain:
                    max_info_gain = info_gain
                    attribute_selected = i
                    threshold_value = attributeValue
                else:
                    continue
        left_subset = []
        right_subset = []
        for line in records:
            if line['attributes'][attribute_selected] == threshold_value:
                left_subset.append(line)
            else:
                right_subset.append(line)

        return attribute_selected, threshold_value, left_subset, right_subset

    def tree_growth(self, records, attributes):
        """
        This function grows the Decision Tree recursively until the stopping
        criterion is met. Please see textbook p164 for more details

        This function should return a TreeNode
        """


        # Your code here
        # Hint-1: Test whether the stopping criterion has been met by calling function stopping_cond()
        # Hint-2: If the stopping criterion is met, you may need to create a leaf node
        # Hint-3: If the stopping criterion is not met, you may need to create a
        #         TreeNode, then split the records into two parts and build a
        #         child node for each part of the subset

        # If the stopping criterion is met
        if self.stopping_cond(records, attributes):
            leaf = TreeNode(True)
            leaf.val = self.classify(records)
            return leaf

        # If the stopping criterion is not met
        attribute_selected, threshold_value, left_subset, right_subset = self.find_best_split(records, attributes)
        if len(left_subset) == 0 or len(right_subset) == 0:
            leaf = TreeNode(True)
            leaf.val = self.classify(records)
            return leaf
        node = TreeNode()
        node.attribute = attribute_selected
        node.val = threshold_value

        if len(left_subset) is not 0:
            node.left = self.tree_growth(left_subset, attributes)
        if len(right_subset) is not 0:
            node.right = self.tree_growth(right_subset, attributes)

        return node


    def entropy(self, records):
        """
        For calculate the entropy of a given data set
        """
        total_number = len(records)
        label_counts = {}
        for line in records:
            current_label = line['label']
            if current_label not in label_counts.keys():
                label_counts[current_label] = 0
            label_counts[current_label] += 1
        entropy = 0.0
        for label in label_counts:
            prob = float(label_counts[label])/total_number
            entropy -= prob * log(prob, 2)
        return entropy



