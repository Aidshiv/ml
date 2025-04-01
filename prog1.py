import pandas as pd
from sklearn.datasets import fetch_california_housing
import matplotlib.pyplot as plt
import seaborn as sns

california_housing = fetch_california_housing()
print(california_housing.DESCR)

df = pd.DataFrame(california_housing.data, columns=california_housing.feature_names)
df['target'] = california_housing.target

print("First 5 rows of the dataset:")
print(df.head())

def plot_histograms(df):
    df.hist(bins=30, figsize=(12, 10))
    plt.suptitle("Histograms of Numerical Features", fontsize=16)
    plt.show()

def plot_boxplots(df):
    plt.figure(figsize=(12, 10))
    for i, feature in enumerate(df.columns):
        plt.subplot(3, 4, i+1)
        sns.boxplot(df[feature])
        plt.title(f'Box Plot of {feature}')
    plt.tight_layout()
    plt.show()

plot_histograms(df)
plot_boxplots(df)

print("Outliers Detection:")
outliers_summary = {}
numerical_features = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
for feature in numerical_features:
    Q1 = df[feature].quantile(0.25)
    Q3 = df[feature].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    outliers = df[(df[feature] < lower_bound) | (df[feature] > upper_bound)]
    outliers_summary[feature] = len(outliers)
    print(f"{feature}: {len(outliers)} outliers")
