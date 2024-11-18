import pandas as pd
import datetime as dt
import re
import functools as fc

# Loading data to file
countries = pd.read_csv(r'C:\UfoProject\data\countries.csv',sep=';')
dataframe = pd.read_csv(r'C:\UfoProject\data\complete.csv',sep=',',on_bad_lines='warn')

# countries dataframe preparing for merge
countries['name'] = countries['name'].astype('str')
countries['name'] = countries['name'].str.lower()
countries['code'] = countries['code'].str.lower()
countries['filled_country'] = countries['name']

# Functions for data conversions
# Filter data to be only numbers, dots and comma
def regex_match(a):
    if re.search("[0123456789,.]", a):
        return True
    else:
        return False

# If array empty paste dummy data
def check_if_array_empty(a):
    if len(a) > 0:
        return a
    else:
        return ['0','.','0']

# Check if cell contains string, if yes return value
def country_check(a,string,value):
    if string in a:
        return value
    else:
        return a


# Datetime column cleanups
dataframe['date_changes'] = dataframe['datetime'].str.split(" ")
dataframe['new_date'] = dataframe['date_changes'].apply(lambda a: a[0])
dataframe['new_hour'] = dataframe['date_changes'].apply(lambda a: a[1])
dataframe['new_hour'] = dataframe['new_hour'].apply(lambda a: a + ':00')
dataframe['new_date'] = dataframe['new_date'].astype('datetime64[ns]')
dataframe['new_hour'] = dataframe['new_hour'].astype('timedelta64[ns]')
dataframe['hour_cleaned'] = dataframe['new_hour'].mask(dataframe['new_hour'] >= dt.timedelta(hours=24), dataframe['new_hour'] - dt.timedelta(hours=24))
dataframe['final_date'] = dataframe['new_date'] + dataframe['hour_cleaned']
dataframe['datetime'] = dataframe['final_date']
dataframe = dataframe.drop(columns = ['date_changes','new_date','new_hour','hour_cleaned','final_date'])

# Changing types of clomuns which don't need cleanup
dataframe['date posted'] = dataframe['date posted'].astype('datetime64[ns]')
dataframe['comments'] = dataframe['comments'].astype('str')

# Latitude column cleanups
dataframe['latitude'] = dataframe['latitude'].astype('str')
dataframe['latitude'] = dataframe['latitude'].str.strip()
dataframe['lat_splited'] = dataframe['latitude'].str.split('')
dataframe['lat_filtered'] = dataframe['lat_splited'].apply(
    lambda x: list(filter(
        lambda a:  regex_match(a),x
    ))
)
dataframe['lat_final'] = dataframe['lat_filtered'].apply(
    lambda x: fc.reduce(
        lambda a,b: a+b,x)
)
dataframe['lat_final'] = dataframe['lat_final'].astype('float')
dataframe['latitude'] = dataframe['lat_final']


# Duration column cleanups
dataframe['duration (seconds)'] = dataframe['duration (seconds)'].astype('str')
dataframe['duration (seconds)'] = dataframe['duration (seconds)'].str.strip()
dataframe['dur_splited'] = dataframe['duration (seconds)'].str.split('')
dataframe['dur_filtered'] = dataframe['dur_splited'].apply(
    lambda x: list(filter(
        lambda a:  regex_match(a),x
    ))
)
dataframe['dur_filtered'] = dataframe['dur_filtered'].apply(lambda x: check_if_array_empty(x))
dataframe['dur_final'] = dataframe['dur_filtered'].apply(
    lambda x: fc.reduce(
        lambda a,b: a+b,x)
)
dataframe['duration (seconds)'] = dataframe['dur_final']
dataframe['duration (seconds)'] = dataframe['duration (seconds)'].astype('float')
dataframe = dataframe.drop(columns = ['lat_splited','lat_filtered','lat_final','dur_splited','dur_filtered','dur_final'])


# countries column filling with data from cities
dataframe['state_check'] = dataframe['state'].isnull()
dataframe['country_check'] = dataframe['country'].isnull()

t_df = dataframe.copy()
t_df = t_df[t_df['country_check'] == True]
t_df['city'] = t_df['city'].astype('str')
t_df['city'] = t_df['city'].str.strip()
t_df['city_new'] = t_df['city'].str.split('(')
t_df['filtr'] = t_df['city_new'].apply(lambda a: len(a) > 1)
t_df = t_df[t_df['filtr'] == True ]
t_df['filled_country'] = t_df['city_new'].apply(lambda a: a[max(len(a)-1,0)])
t_df['filled_country'] = t_df['filled_country'].astype('str')
t_df['filled_country'] = t_df['filled_country'].str.rstrip(')')
t_df['filled_country'] = t_df['filled_country'].str.lower()
t_df['filled_country'] = t_df['filled_country'].apply(lambda a: country_check(a,'uk/','united kingdom'))
t_df['filled_country'] = t_df['filled_country'].apply(lambda a: country_check(a,'australia','australia') )

# merging with countires table 
result = pd.merge(t_df,countries,on='filled_country',how='left')
result['check'] = result['name'].isnull()
result['country'] = result['code']
result = result.drop(columns=['city_new','filtr','filled_country','id','code','name','continent','check'])

result['country_check'] = result['country'].isnull()
result_2 = result.copy()
result_2 = result_2[result_2['country_check']== True]
result_2 = result_2[result_2['state_check']==False]
result_2['country'] = 'us'

result = result[result['country_check'] == False]
final = pd.concat([result,result_2])

# final merge after filling columns
m_df = pd.merge(dataframe,final, on=['datetime','city','duration (seconds)','state','shape','duration (hours/min)','comments','date posted','latitude','longitude','state_check'],how='left')
m_df['country_x'] = m_df['country_x'].mask(m_df['country_check_y'] == False,m_df['country_y'])
m_df = m_df.drop(columns=['state_check','country_check_x','country_y','country_check_y'])
m_df = m_df.rename(columns={'country_x':'country'})

# filling null countires with state entry with 'us'
m_df['country_check'] = m_df['country'].isnull()
m_df['state_check'] = m_df['state'].isnull()
m_df_2 = m_df.copy()
m_df = m_df[m_df['country_check']== False]
m_df_2 = m_df_2[m_df_2['country_check']== True]
m_df_2['country'] = m_df_2['country'].mask(m_df_2['state_check']==False,'us')
m_df = pd.concat([m_df,m_df_2])
m_df = m_df.sort_index()
m_df['country_check'] = m_df['country'].isnull()
m_df = m_df.drop(columns=['country_check','state_check'])


# shape column cleanups
m_df['shape'] = m_df['shape'].mask(m_df['shape'] == 'flare','light')
m_df['shape'] = m_df['shape'].mask(m_df['shape'] == 'round','sphere')
m_df['shape'] = m_df['shape'].mask(m_df['shape'] == 'delta','triangle')
m_df['shape'] = m_df['shape'].mask(m_df['shape'] == 'pyramid','triangle')
m_df['shape'] = m_df['shape'].mask(m_df['shape'] == 'changed','changing')
m_df['shape'] = m_df['shape'].mask(m_df['shape'] == 'disk','circle')
m_df['shape'] = m_df['shape'].mask(m_df['shape'] == 'flash','light')
m_df['shape'] = m_df['shape'].mask(m_df['shape'] == 'teardrop','oval')
m_df['shape'] = m_df['shape'].mask(m_df['shape'] == 'egg','oval')
m_df['shape'] = m_df['shape'].mask(m_df['shape'] == 'unknown','other')
m_df['shape'] = m_df['shape'].mask(m_df['shape'] == 'hexagon','other')
m_df['shape'] = m_df['shape'].mask(m_df['shape'] == 'crescent','other')
m_df['shape'] = m_df['shape'].mask(m_df['shape'] == 'dome','other')
m_df['shape_check'] = m_df['shape'].isnull()
m_df['shape'] = m_df['shape'].mask(m_df['shape_check'] == True,'other')
m_df['shape_check'] = m_df['shape'].isnull()
m_df = m_df.drop(columns=['shape_check'])

# Renaming columns, preparing to save final files
m_df = m_df.rename(columns={'datetime':'observation_datestamp','country':'country_id','shape':'shape_type','duration (seconds)':'duration_in_sec','duration (hours/min)':'duration_string','state':'state_id','date posted':'date_posted'})
m_df.to_csv(r'data/complete_cleaned.csv',sep=';')
m_df.to_parquet(r'data/complete_cleaned.parquet')




