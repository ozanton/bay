## Customer churn
***Machine Learning with a Teacher***

### Brief description of the project
We need to predict whether a customer will leave the bank in the near future or not.  
We are provided with historical data on customer behavior and customer termination with the bank. 

Objective:
- To build a model with an extremely large value of *F1*-measure. 

### The libraries used are
*pandas*, *numpy*, *sklearn.(tree, ensemble, metrics, linear_model, compose, pipeline, impute, preprocessing, model_selection, utils)*, *tqdm*

### Description of the work done
Classification task.
The primary data analysis and data preprocessing was done - we dealt with data gaps, checked for duplicates, removed attributes that were not necessary for modeling.  
Using `pipeline` data is divided into training, validation samples and trained three models: Logistic Regression, Random Forest and Decision Tree.  
Class imbalances were eliminated and class weights were balanced.  
Hyperparameter selection performed.  
Metrics - F1-Score, ROC AUC.