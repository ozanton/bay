## Car Value Determination
***Machine Learning, Numerical Methods***

### Brief description of the project
It is required to predict the cost of a car according to its technical characteristics, and configuration.
Objective:
- To build a model to determine the cost of a car.
The customer is important:
- quality of prediction;
- prediction speed;
- training time.

### Used libraries
*pandas*, *numpy*, *matplotlib*, *sklearn.(compose, pipeline, impute, preprocessing, model_selection, metrics, linear_model)*, *lightgbm*, *catboost*

### Description of work done
Preprocessing of data, control of omissions, duplicates and outliers was executed.  
Three models were trained: linear regression, lightgbm and catboost.  
We obtained the results of cross-validation testing of the models and made a conclusion about the appropriate model in terms of quality and speed of training and prediction.