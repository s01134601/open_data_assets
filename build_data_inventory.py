import requests
import pandas as pd

# each dataset has a unique identifier
id_list = ['b5sv-ucke', 'bgq7-v7ms', 'jmub-3h99', 'h548-w4m3', 'qh8j-6k63', 'b5sv-ucke', 'th3p-us5u', 'bsk3-nwf8',
           '4w4k-aek3', 'haj6-m47h', 'cjeq-bs86', 'y8dh-5w5c', 'f9wk-2wb9', 'jxhb-ebn5', 'ag43-fvd7', '32au-zaqn']
# these are information we want from metadata
cols = ['id', 'name', 'attribution', 'dataUpdatedAt', 'createdAt', 'webUri', 'dataUri', 'description', 'license',
        'customFields.Department.Publishing Department', 'customFields.Publishing Details.Data change frequency',
        'customFields.Publishing Details.Publishing frequency']
# get a list of all the datasets and their info before counting how many rows and cols are in each dataset
df_total = pd.DataFrame()
row_counts = []
col_counts = []
for ids in id_list:
    url = 'https://datacatalog.cookcountyil.gov/api/views/metadata/v1/{id}'.format(id=ids)
    response = requests.get(url)
    data = response.json()
    try:
        df = pd.json_normalize(data)[cols]
    except KeyError:
        data_dict = {}
        for keys in cols:
            data_dict[keys] = data.get(keys)
        df = pd.DataFrame(data_dict, index=[0])
    data_url = df['dataUri'].iloc[0]
    pd.set_option('display.max_columns', None)
    if data_url is not None:
        total_rows = 0
        offset = 0
        batch_size = 5000
        while True:
            response2 = requests.get(data_url + '.json?$offset={}&$limit={}'.format(offset, batch_size))
            data2 = response2.json()
            row_count = len(data2)
            col_count = len(data2[0])
            total_rows += row_count
            offset += batch_size
            if row_count < batch_size:
                break
        df['rows'] = total_rows
        df['columns'] = col_count
    else:
        df['rows'] = 0
        df['columns'] = 0
    df_total = pd.concat([df_total, df], axis=0)

pd.set_option('display.max_columns', None)
df_total.rename({'customFields.Department.Publishing Department': 'Publishing Department',
                 'customFields.Publishing Details.Data change frequency': 'Data change frequency',
                 'customFields.Publishing Details.Publishing frequency': 'Publishing frequency'}, axis=1,
                inplace=True)
print(df_total)
writer = pd.ExcelWriter('/Users/bling/Documents/21-22/data science/cook county/open_data_assets_oup.xlsx')
df_total.to_excel(writer, sheet_name='oup_asset', index=False)
writer.save()
