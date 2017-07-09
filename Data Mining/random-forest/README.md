Name: Yufeng Yuan
UML ID: 01506240
E-mail: Yufeng_Yuan@student.uml.edu

## Decision Tree

For decision tree, I used the information-gain for finding the best split point. Before split the data, I’ll calculate all possible attribute value’s information gain and find the maximum one, and select its attribute and its value as the best split point.
For calculate the information gain, I created a new function called “entropy”, giving a set of records, this function will return the entropy of the set.

**Running Command:**
python src/main.py -m 0 -t data/mushrooms_train.data -e data/mushrooms_test.data

## Random Forest
For random forest, I did the following step:
1.	Draw n bootstrap sample for training.
2.	For each bootstrap sample, randomly selected m attributes (I choose m = 10)
3.	Training each sample and its attributes by decision tree.
4.	Predict the test set in each tree, and select the majority result.

**Running Command:**
python src/main.py -m -1 -t data/mushrooms_train.data -e data/mushrooms_test.data -m 20


## Other infomation see FinalReport.pdf
