import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.datasets import load_boston
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import seaborn as sns

boston = load_boston()
X, y = boston.data, boston.target
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
lr_model = LinearRegression()
lr_model.fit(X_train, y_train)
y_pred = lr_model.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)
r2 = r2_score(y_test, y_pred)
print(f"Linear Regression:")
print(f'Mean Squared Error: {mse}')
print(f'Root Mean Squared Error: {rmse}')
print(f'R-squared: {r2}')
plt.scatter(X_test[:, 5], y_test, color='blue', label='Actual')
plt.plot(X_test[:, 5], y_pred, color='red', label='Predicted')
plt.title("Linear Regression: Boston Housing (RM vs Price)")
plt.xlabel("Average Number of Rooms (RM)")
plt.ylabel("House Prices")
plt.legend()
plt.show()

df = sns.load_dataset('mpg').dropna()
X_poly = df[['horsepower', 'weight']]
y_poly = df['mpg']
X_train_poly, X_test_poly, y_train_poly, y_test_poly = train_test_split(X_poly, y_poly, test_size=0.2, random_state=42)
poly = PolynomialFeatures(degree=3)
X_train_poly_transformed = poly.fit_transform(X_train_poly)
X_test_poly_transformed = poly.transform(X_test_poly)
poly_model = LinearRegression()
poly_model.fit(X_train_poly_transformed, y_train_poly)
y_pred_poly = poly_model.predict(X_test_poly_transformed)
mse_poly = mean_squared_error(y_test_poly, y_pred_poly)
rmse_poly = np.sqrt(mse_poly)
r2_poly = r2_score(y_test_poly, y_pred_poly)
print(f"\nPolynomial Regression:")
print(f'Mean Squared Error: {mse_poly}')
print(f'Root Mean Squared Error: {rmse_poly}')
print(f'R-squared: {r2_poly}')
plt.scatter(X_test_poly['horsepower'], y_test_poly, color='blue', label='Actual')
plt.scatter(X_test_poly['horsepower'], y_pred_poly, color='red', label='Predicted')
plt.title("Polynomial Regression: Auto MPG (Horsepower vs MPG)")
plt.xlabel("Horsepower")
plt.ylabel("Miles per Gallon (MPG)")
plt.legend()
plt.show()
