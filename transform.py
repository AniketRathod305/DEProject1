import pandas as pd
from extract import extract_data


def transform_data():
    df = extract_data()
    df.drop('Unnamed: 0', axis=1, inplace=True)
    df[['City', 'Country']] = df['Location'].str.split(',', expand=True)
    df.drop('Location', axis=1, inplace=True)
    # df['Hire_Date'] = pd.to_datetime(df['Hire_Date']).dt.strftime('%Y/%m/%d')
    df["Hire_Date"] = pd.to_datetime(df["Hire_Date"], format="%d-%m-%Y")

    # print(df.head(10))
    # print(df.columns)
    return df


# transform_data()
