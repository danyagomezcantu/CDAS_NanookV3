#%% Lectura de las bases de datos

import pandas as pd

trayectoria="C:/Users/Danya/OneDrive - INSTITUTO TECNOLOGICO AUTONOMO DE MEXICO/Escritorio/CDAS_ServicioSocial/"

p1=pd.read_csv(trayectoria+'Datos FBpages/CC_FBpages_2013y2014.csv')
p2=pd.read_csv(trayectoria+'Datos FBpages/CC_FBpages_2015.csv')
p3=pd.read_csv(trayectoria+'Datos FBpages/CC_FBpages_2016.csv')
p4=pd.read_csv(trayectoria+'Datos FBpages/CC_FBpages_2017.csv')
p5=pd.read_csv(trayectoria+'Datos FBpages/CC_FBpages_2018.csv')
p6=pd.read_csv(trayectoria+'Datos FBpages/CC_FBpages_2019.csv')
p7=pd.read_csv(trayectoria+'Datos FBpages/CC_FBpages_2020.csv')
p8=pd.read_csv(trayectoria+'Datos FBpages/CC_FBpages_2021.csv')
p9=pd.read_csv(trayectoria+'Datos FBpages/CC_FBpages_2022.csv')

dataframes = [p1,p2,p3,p4,p5,p6,p7,p8,p9]
data_fbpages = pd.concat(dataframes)

del p1,p2,p3,p4,p5,p6,p7,p8,p9,dataframes

#%% Creamos count y sum
data_fbpages['Post Created Date'] = pd.to_datetime(data_fbpages['Post Created Date'])
data_fbpages.set_index('Post Created Date', inplace=True)
data_fbpages['Total Interactions'] = pd.to_numeric(data_fbpages['Total Interactions'].str.replace(',', ''))

count_df = data_fbpages.groupby(['Post Created Date', 'Page Category'])['Total Interactions'].count()
count_df = count_df.reset_index()
count_df = count_df.rename(columns={'Total Interactions': 'Num Posts'})

sum_df = data_fbpages.groupby(['Post Created Date', 'Page Category'])['Total Interactions'].sum()
sum_df = sum_df.reset_index()

#%% Supercategorías

supercategories=['Arts and Entertainment',
                 'Business and Industry',
                 'Education and Research',
                 'Government',
                 'Media',
                 'News',
                 'Non-Profit and Community Organizations',
                 'Politics',
                 'Science and Technology']

supercategories_dict={'Arts and Entertainment':['ENTERTAINMENT_SITE','VIDEO_CREATOR','TOPIC_ARTS_ENTERTAINMENT','BANDS_MUSICIANS','ARTIST','CULTURAL_CENTER','GAMING_VIDEO_CREATOR','AUTHOR','ART','TOPIC_EVENT','FESTIVAL','TOPIC_MUSEUM'],
                 'Business and Industry':['DIGITAL_CREATOR','PRODUCT_SERVICE','TOPIC_BUSINESS_SERVICES','ENERGY_COMPANY','AGRICULTURAL_SERVICE','CONSULTING_COMPANY','AGRICULTURE','COMPANY','TOPIC_PUBLISHER','FINANCIAL_SERVICES','INDUSTRIALS_COMPANY','TELECOM','LOCAL_SERVICES','REF_SITE','BIZ_SITE','AGRICULTURAL_COOPERATIVES','ENTREPRENEUR','SOLAR_ENERGY_SERVICE','ADVERTISING_MARKETING','BANK','SHOPPING_MALL','PUBLIC_UTILITY','INSURANCE_COMPANY','TOPIC_LIBRARY','TRAVEL_SITE','BUSINESS_CENTER','TOPIC_BOOK_STORE','ADVERTISING_AGENCY'],
                 'Education and Research':['UNIVERSITY','EDUCATION_COMPANY','EDU_SITE','EDUCATIONAL_RESEARCH','SCHOOL','COMMUNITY_COLLEGE','CAMPUS_BUILDING','LANGUAGE_SCHOOL','PRIVATE_SCHOOL'],
                 'Government':['GOVERNMENT_ORGANIZATION','CONSULATE_EMBASSY','PUBLIC_SERVICES_GOVERNMENT','GOVERNMENT_BUILDING','CITY_HALL','GOVERNMENT_OFFICIAL','GOV_SITE'],
                 'Media':['ACTIVITY_GENERAL','TV_CHANNEL','MAGAZINE','MEDIA','RADIO_STATION','BROADCASTING_MEDIA_PRODUCTION','TV_SHOW','PERSONAL_BLOG','NEWS_PERSONALITY','SOCIETY_SITE','WEBSITE','TV_NETWORK','BLOGGER','TOPIC_JUST_FOR_FUN','MOVIE_WRITER','MOVIE_THEATRE','REGIONAL_SITE','MEDIA_SHOW'],
                 'News':['MEDIA_NEWS_COMPANY','NEWS_SITE','TOPIC_NEWSPAPER','JOURNALIST'],
                 'Non-Profit and Community Organizations':['NON_PROFIT','COMMUNITY','NGO','COMMUNITY_ORGANIZATION','CAUSE','ORG_GENERAL','PUBLIC_SERVICES','RELIGIOUS_ORGANIZATION','CHARITY_ORGANIZATION','COMMUNITY_SERVICES','SOCIAL_SERVICES','YOUTH_ORGANIZATION','LABOR_UNION'],
                 'Politics':['LOCAL','POLITICIAN','PERSON','POLITICAL_ORGANIZATION','POLITICAL_PARTY','CITY'],
                 'Science and Technology':['ENVIRONMENTAL_CONSERVATION','SCIENCE_SITE','SCIENCE_ENGINEERING','HEALTH_SITE','ENVIRONMENTAL_CONSULTANT','MEDICAL_HEALTH','SCIENTIST','HEALTH_BEAUTY','SCIENCE_MUSEUM','ENVIRONMENTAL','HOSPITAL','BIOTECHNOLOGY']}

def map_category(category):
    for supercategory, categories in supercategories_dict.items():
        if category in categories:
            return supercategory
    return None

# Unir categorías con supercategorías

data_fbpages['Supercategory'] = data_fbpages['Page Category'].apply(map_category)
data_fbpages = data_fbpages.dropna(subset=['Supercategory'])

count_df['Supercategory'] = count_df['Page Category'].apply(map_category)
sum_df['Supercategory'] = sum_df['Page Category'].apply(map_category)

# Eliminar las categorías de páginas que no están asignadas a supercategorías
# (que no tengan más de 500 publicaciones en 10 años)
# y columnas innecesarias

count_df = count_df.dropna(subset=['Supercategory'])
count_df = count_df.drop('Page Category', axis=1)

sum_df = sum_df.dropna(subset=['Supercategory'])
sum_df = sum_df.drop('Page Category', axis=1)

# Agrupar por supercategoría y fechas
count_df = count_df.groupby([pd.Grouper(key='Post Created Date', freq='M'), 'Supercategory']).sum().reset_index()
sum_df = sum_df.groupby([pd.Grouper(key='Post Created Date', freq='M'), 'Supercategory']).sum().reset_index()

#%%

# Get average number of interactions per publication
final_df = pd.merge(count_df, sum_df, on=['Supercategory', 'Post Created Date'])
final_df['Avg Interactions per Post'] = final_df['Total Interactions'] / final_df['Num Posts']
final_df.drop(['Num Posts', 'Total Interactions'], axis=1, inplace=True)

#%% Hitos

data_hitos1 = pd.read_csv("C:/Users/Danya/OneDrive - INSTITUTO TECNOLOGICO AUTONOMO DE MEXICO/Escritorio/CDAS_ServicioSocial/Hitos2022_LATAM.csv")
data_hitos1 = data_hitos1.dropna()

data_hitos2 = pd.read_csv("C:/Users/Danya/OneDrive - INSTITUTO TECNOLOGICO AUTONOMO DE MEXICO/Escritorio/CDAS_ServicioSocial/Hitos2022_Internacional.csv")
data_hitos2 = data_hitos2.dropna()

data_hitos3 = pd.read_csv("C:/Users/Danya/OneDrive - INSTITUTO TECNOLOGICO AUTONOMO DE MEXICO/Escritorio/CDAS_ServicioSocial/Hitos2022_Reportes.csv")
data_hitos3 = data_hitos3[['Organización', 'Título', 'Categoría', 'Fecha', 'Link Reporte','Conclusiones','Recomendaciones','Palabras clave de conclusiones y recomendaciones','Imagen del Reporte']]
data_hitos3 = data_hitos3.dropna()

data_hitos1["Fecha1"] = data_hitos1["Fecha (DD/MM/YYYY)"].str.split("/")
data_hitos1 = data_hitos1[data_hitos1["Fecha1"].map(len) == 3].reset_index(drop=True)

data_hitos2["Fecha1"] = data_hitos2["Date (DD/MM/YYYY)"].str.split("/")
data_hitos2 = data_hitos2[data_hitos2["Fecha1"].map(len) == 3].reset_index(drop=True)

data_hitos3["Fecha1"] = data_hitos3["Fecha"].str.split("/")
data_hitos3 = data_hitos3[data_hitos3["Fecha1"].map(len) == 3].reset_index(drop=True)

for i in range(len(data_hitos1.index)):
    data_hitos1["Fecha1"][i] = data_hitos1["Fecha1"][i][2] + "-" + data_hitos1["Fecha1"][i][1]

df_hitos1 = data_hitos1[['Fecha1', 'Hito', 'País']]
df_hitos1.rename(columns = {'Hito':'Hito', 'Fecha1':'Date', 'País':'País u organización'}, inplace = True)

for i in range(len(data_hitos2.index)):
    data_hitos2["Fecha1"][i] = data_hitos2["Fecha1"][i][2] + "-" + data_hitos2["Fecha1"][i][1]

df_hitos2 = data_hitos2[['Fecha1', 'Milestone']]
df_hitos2 = df_hitos2.assign(País = 'Global')
df_hitos2.rename(columns = {'Milestone':'Hito', 'Fecha1':'Date', 'País':'País u organización'}, inplace = True)

for i in range(len(data_hitos3.index)):
    data_hitos3["Fecha1"][i] = data_hitos3["Fecha1"][i][2] + "-" + data_hitos3["Fecha1"][i][1]

df_hitos3 = data_hitos3[['Fecha1', 'Título', 'Organización']]
df_hitos3.rename(columns = {'Título':'Hito', 'Fecha1':'Date', 'Organización':'País u organización'}, inplace = True)

hitos = [df_hitos1, df_hitos2, df_hitos3]
data_hitos = pd.concat(hitos)

del df_hitos1, df_hitos2, df_hitos3, hitos, data_hitos1, data_hitos2, data_hitos3, i

data_hitos = data_hitos.groupby('Date').agg(lambda x: ', '.join(x)).reset_index()

# Aquí falta que los datos de "País u organización" no se repita

#%% Unimos hitos con count

count_df['Date']=count_df['Post Created Date'].dt.strftime('%Y-%m')

count_hitos = pd.merge(count_df, data_hitos, on='Date', how='left')
count_hitos = count_hitos.drop('Post Created Date', axis=1)
count_hitos['Hito'].fillna('No hay hitos relevantes', inplace=True)
count_hitos['País u organización'].fillna('No hay hitos relevantes', inplace=True)

count_hitos['Date'] = pd.to_datetime(count_hitos['Date'])
#count_hitos.set_index(['Fecha', 'Num Posts'], inplace=True)

#%% Unimos hitos con sum

sum_df['Date']=sum_df['Post Created Date'].dt.strftime('%Y-%m')

sum_hitos = pd.merge(sum_df, data_hitos, on='Date', how='left')
sum_hitos = sum_hitos.drop('Post Created Date', axis=1)
sum_hitos['Hito'].fillna('No hay hitos relevantes', inplace=True)
sum_hitos['País u organización'].fillna('No hay hitos relevantes', inplace=True)

sum_hitos['Date'] = pd.to_datetime(sum_hitos['Date'])
#sum_hitos.set_index(['Fecha', 'Total Interactions'], inplace=True)

#%% Unimos hitos con final (promedio de interacciones)

final_df['Date']=final_df['Post Created Date'].dt.strftime('%Y-%m')

final_hitos = pd.merge(final_df, data_hitos, on='Date', how='left')
final_hitos = final_hitos.drop('Post Created Date', axis=1)
final_hitos['Hito'].fillna('No hay hitos relevantes', inplace=True)
final_hitos['País u organización'].fillna('No hay hitos relevantes', inplace=True)
final_hitos['Date'] = pd.to_datetime(final_hitos['Date'])
#final_hitos.set_index(['Fecha', 'Avg Interactions per Post'], inplace=True)

#%% Versión anterior de las gráficas

""" 

# Gráfica 1: Número de publicaciones por Supercategoría

import seaborn as sns


plt.figure(dpi=500)
ax = sns.lineplot(x="Date", y="Num Posts", hue="Supercategory", data=count_hitos)

ax.set(title="Total Number of Posts for each Supercategory", xlabel="Date", ylabel="Number of Posts")
l=ax.legend(title='Supercategory\n', bbox_to_anchor=(1, 1.02), fontsize=8)
plt.setp(l.get_title(), multialignment='center')

plt.show()


# Gráfica 2: Interacciones totales por Supercategoría

plt.figure(dpi=500)
ax = sns.lineplot(x="Date", y="Total Interactions", hue="Supercategory", data=sum_hitos)

ax.set(title="Total Interactions for each Supercategory", xlabel="Date", ylabel="Total Interactions")
l=ax.legend(title='Supercategory\n', bbox_to_anchor=(1, 1.02), fontsize=8)
plt.setp(l.get_title(), multialignment='center')

plt.show()


# Gráfica 3: Interacciones promedio por publicación para cada Supercategoría

plt.figure(dpi=500)
ax = sns.lineplot(x="Date", y="Avg Interactions per Post", hue="Supercategory", data=final_hitos)

ax.set(title="Average Interactions for each Supercategory", xlabel="Date", ylabel="Average Interactions per Post")
l=ax.legend(title='Supercategory\n', bbox_to_anchor=(1, 1.02), fontsize=8)
plt.setp(l.get_title(), multialignment='center')

plt.show()

"""

#%% Gráficas actualizadas

# Gráfica 1: Número de publicaciones por Supercategoría

import pandas as pd
import plotly.express as px

# Create a line plot with plotly express and count_hitos
fig = px.line(count_hitos, x='Date', y='Num Posts', color='Supercategory',
              labels={'Num Posts': 'Number of Posts'},
              title='Total Number of Posts for each Supercategory')

# Customize tooltip text
text=[count_hitos['Hito'].tolist(), count_hitos['País u organización'].tolist()]
fig.update_traces(hovertemplate='Date: %{x}<br>Number of Posts: %{y}<br>Hito: %{text[0]}<br>País u organización: %{text[1]}')

downloads = "C:\\Users\Danya\\Downloads"

# Save the plot as an HTML file in the downloads folder
html_file_path = downloads+"\interactive_num_posts.html"
fig.write_html(html_file_path)

# Gráfica 2: Interacciones totales por Supercategoría
# Gráfica 3: Interacciones promedio por publicación para cada Supercategoría