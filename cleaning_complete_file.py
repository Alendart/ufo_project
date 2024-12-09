import pandas as pd
import datetime as dt
import re
import functools as fc
import pycountry

# Loading data to file
countries = pd.read_csv(r'C:\Repos\ufo_project\data\additional_files\countries.csv',sep=',')
dataframe = pd.read_csv(r'C:\Repos\ufo_project\data\source_files\complete.csv',sep=',',on_bad_lines='warn')

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

#funkcja do wyszukania nazwy kraju po country_id przy użyciu ISC code
def get_country_name(iso_code):
    if isinstance(iso_code, str):  # Sprawdzamy, czy kod jest łańcuchem znaków
        try:
            iso_code = iso_code.upper()
            country = pycountry.countries.get(alpha_2=iso_code)
            return country.name if country else None
        except KeyError:
            return None
    else:
        return None  # Dla NaN i innych nieprawidłowych danych zwracamy None

def get_country_code(name):
    try:
        return pycountry.countries.lookup(name).alpha_2  # Zwraca kod ISO alpha_2 (np. 'PL')
    except LookupError:
        return None  # Jeśli nie znajdzie kraju, zwraca None

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

# Saving of first version of files
# m_df.to_csv(r'data/complete_cleaned.csv',sep=';')
# m_df.to_parquet(r'data/complete_cleaned.parquet')

m_df["observation_datestamp"] = pd.to_datetime(m_df["observation_datestamp"])
m_df['date']=m_df['observation_datestamp'].dt.date

# adding year column
m_df['date']=pd.to_datetime(m_df['date'],format="%Y-%m-%d")
m_df['year'] = m_df['date'].dt.year
m_df.drop(columns="observation_datestamp",inplace=True)

m_df['state_id']=m_df['state_id'].str.upper()
m_df['country_id']=m_df['country_id'].str.upper()
m_df['city']=m_df['city'].str.title()

#usuwanie nawiasow wraz z zawartoscia
m_df['city'] = m_df['city'].str.replace(r'\(.*?\)', '', regex=True)

#usuwanie nawiasów otwierających wraz z zawartoscia
m_df['city'] = m_df['city'].str.replace(r'\(.*?', '', regex=True)

#ręczna poprawa powyższych danych
m_df.loc[8194,'country_id']='CA'
m_df.loc[5789,'city']='Unadilla'
m_df.loc[8194,'city']='Ft. Resolution'
m_df.loc[40609,'city']='Tel Aviv'
m_df.loc[40609,'country_id']='IL'
m_df.loc[52860,'city']='London'
m_df.loc[52860,'country_id']='GB'  
m_df.loc[45133,'city']='Iowa City'
m_df.loc[54713, 'city']='Phuket'
m_df.loc[54713, 'country_id']='TH'
m_df.loc[60128, 'city']=''
m_df.loc[60128, 'country_id']='AFG'
m_df.loc[75757,'city']=''   
m_df.loc[78575,'city']=''
m_df.loc[80683,'city']='Midwest'
m_df.loc[80683,'country_id']='US'

#usuwanie nawiasow zamykających + spacji przed
m_df['city'] = m_df['city'].str.replace(r' \)', '', regex=True)

#usuwanie nawiasow zamykających bez spacji
m_df['city'] = m_df['city'].str.replace(r'\)', '', regex=True)

m_df['city'] = m_df['city'].str.strip().str.replace(r'\s+', ' ', regex=True)
m_df['city'] = m_df['city'].str.replace(r'\bNas\b', '', regex=True)

#nowa kolumna country_name, która korzysta z funkcji get_country_name
m_df['country_name'] = m_df['country_id'].apply(get_country_name)

#gdy country_name jest puste, dodajemy info z city
m_df['country_name'] = m_df['country_name'].fillna(m_df['city'])

m_df['id'] = m_df.index + 1

new_order = ['id','date','year','city','state_id','country_id','country_name','shape_type','duration_in_sec','duration_string','comments','date_posted','latitude','longitude']
m_df = m_df[new_order]

#usunięcie niewidzialnych znaków z country_name
m_df['country_name'] = m_df['country_name'].str.strip().str.replace(r'\s+', ' ', regex=True)

#usuwanie znaków znaków innych niż Literowe
m_df= m_df[~m_df['city'].str.contains(r'[^a-zA-Z\s]', na=False)]

#usuwanie znaków zapytania
m_df= m_df[~m_df['city'].str.contains(r'\?', na=False)]

# Uzupełnianie brakujących wartości w kolumnie country_id
m_df['country_id'] = m_df.apply(
    lambda row: get_country_code(row['country_name']) if pd.isna(row['country_id']) else row['country_id'],
    axis=1)

#usuwanie pozycji gdzie country_id nie zostało uzupełnione
m_df= m_df.dropna(subset=['country_id'])

reg_df=pd.read_csv(r"C:\Repos\ufo_project\data\final_file\regions.csv",
                sep=",",
                header=0
                  )

df_merged=m_df.merge(right=reg_df,how='left',left_on='country_id',right_on='alpha-2')

df_merged.drop(columns="country-code",inplace=True)
df_merged.drop(columns="iso_3166-2",inplace=True)
df_merged.drop(columns="intermediate-region",inplace=True)
df_merged.drop(columns="region-code",inplace=True)
df_merged.drop(columns="sub-region-code",inplace=True)
df_merged.drop(columns="intermediate-region-code",inplace=True)

cleared_data = df_merged

cleared_data.to_csv(r"C:\Repos\ufo_project\data\cleared_data_py.csv",index=False)
cleared_data.to_parquet(r"C:\Repos\ufo_project\data\cleared_data_py.parquet",index=False)
