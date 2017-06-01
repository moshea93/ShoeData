import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import style
style.use('ggplot')

def gender_by_company(df, gender):
	df = df[df['gender'] == gender]
#	nosmallfriesdf = df.groupby('brand').filter(lambda x: len(x) > 49)
	statsdf = df.groupby('brand')['price'].describe()
	return statsdf

def get_category(categories, keyword):
	for category in categories:
		if category == keyword:
			return True
	return False

def gender_counts_by_brand(df):
	df = df[df['gender'].apply(lambda x: x in ['M', 'W'])]
	print 'maindf, post-gender: ' + str(df.shape)
	nosmallfriesdf = df.groupby('brand').filter(lambda x: len(x) > 49)
	print 'nosmallfriesdf, after narrowing: ' + str(nosmallfriesdf.shape)
	nosmallfriesdf['W'] = (nosmallfriesdf['gender'] == 'W').astype(int)
	nosmallfriesdf['M'] = (nosmallfriesdf['gender'] == 'M').astype(int)
	brandgendercountdf = nosmallfriesdf[['brand', 'W', 'M']].groupby(['brand']).agg(['sum'])
#	print nosmallfriesdf[['brand', 'gender', 'id']].groupby(['brand', 'gender']).agg(['count'])
	brandgendercountdf.to_csv('brandgendercount.csv', encoding='utf-8')

def means_and_medians_by_brand(df):
	nosmallfriesdf = df[df['gender'].apply(lambda x: x in ['M', 'W'])]
	brands = nosmallfriesdf.groupby('brand').filter(lambda x: len(x) > 49)['brand'].unique()
	df = df[df['brand'].apply(lambda x: x in brands)]
	meansandmediansdf = df[['brand', 'price']].groupby(['brand']).agg(['count', 'mean', 'median'])
	meansandmediansdf.to_csv('meansandmedians.csv', encoding='utf-8')
	return df

def check_it(jsonfile):
	maindf = pd.read_json(jsonfile)
	print 'maindf: ' + str(maindf.shape)

#	maindf = maindf[maindf['brand'] == 'nike']
#	print maindf['name']
#	print maindf.shape
#	maindf = maindf[maindf['categories'].apply(lambda x: get_category(x, 'Shoes & Bags'))]
#	print maindf['name']
#	print maindf.shape

#	print maindf[maindf['brand'] == 'Tribal T-Shirts'][['name', 'categories']]
#	print brandsdf.shape

#	maindf[(maindf['brand'] == 'nike') & (maindf['price'] > 200)].to_csv('nike200plus.csv', encoding='utf-8')

	#brand | count of items | price mean | price median
	maindf = means_and_medians_by_brand(maindf)
	print 'maindf, after reducing brands: ' + str(maindf.shape)

	#overview by brand and gender
	gender_by_company(maindf, 'M').to_csv('menstats.csv', encoding='utf-8')
	gender_by_company(maindf, 'W').to_csv('womenstats.csv', encoding='utf-8')
	
	#brand | count men records | count women records
	gender_counts_by_brand(maindf)

	
if __name__ == '__main__':
	check_it('output.json')
