import glob
import os
import json
import pandas as pd

def flatten(data, attribute):
	currdata = [item for item in data if attribute in item]
	currdf = pd.io.json.json_normalize(currdata, record_path=attribute, meta='id', record_prefix=attribute + '.')
#	print attribute + ': ' + str(currdf.shape) + ', ' + str(len(currdf.groupby('id')))
	return currdf

def data_frame_to_csv_and_json(df):
	subset = df.loc[0:499,:]
	subset.to_csv('cleaned_sample.csv', encoding='utf-8')
	df.to_csv('cleaned_entries.csv', encoding='utf-8')
#	df.to_json('cleaned_entries.json')

def get_brand(row):
	if row['brand'].lower() == row['manufacturer'].lower():
		return row['brand']
	elif row['brand'] != '' and row['manufacturer'] != '':
		return row['brand'] + ', ' + row['manufacturer']
	elif row['brand'] != '':
		return row['brand']
	else:
		return row['manufacturer']

def ACTUALLY_shoes(row):
	for category in row['categories']:
		if 'shoes' in category.lower():
			return True
	return False

def json_input_to_dataframe(jsonfile):
	basePath = os.path.dirname(os.path.abspath(__file__))
	maindf = pd.read_json(basePath + '/' + jsonfile)
	print 'maindf: ' + str(maindf.shape)
	
	with open(jsonfile) as json_data:
		data = json.load(json_data)
	
	##bring subcategories of "prices" to category level
	pricesdf = flatten(data, 'prices')
	maindf = maindf.drop('prices', axis=1)
	maindf = pd.merge(maindf, pricesdf, how='inner', on='id')
#	print 'maindf, after prices join: ' + str(maindf.shape)
	
	##SourceURL reduced to homepage
	##prices.dateSeen reduced to first date seen
	##Brand/manufacturer condensed
	maindf['sourceURLs'] = maindf['sourceURLs'].apply(lambda x:x[0].split('/')[2])
	maindf['prices.dateSeen'] = maindf['prices.dateSeen'].apply(lambda x:x[0])
	maindf['brand'] = maindf['brand'].fillna(value='')
	maindf['manufacturer'] = maindf['manufacturer'].fillna(value='')
	maindf['brand'] = maindf.apply(get_brand, axis=1)
	
	##Get rid of products with a potential mistaken cost listing
	maindf['Possible Outlier'] = (maindf.groupby(['id'])['prices.amountMin'].transform(max) / maindf.groupby(['id'])['prices.amountMin'].transform(min) > 5)
	maindf = maindf[maindf['Possible Outlier'] != True]
	maindf = maindf.drop('Possible Outlier', axis=1)

	##narrow to one price per product: most recent then cheapest
	idx = maindf.groupby(['id'])['prices.dateSeen'].transform(max) == maindf['prices.dateSeen']
	maindf = maindf[idx]
	idx2 = maindf.groupby(['id'])['prices.amountMin'].transform(min) == maindf['prices.amountMin']
	maindf = maindf[idx2]
	##if tied on most recent and cheapest just pick one
	maindf = maindf.groupby(['id'], as_index=False).first()
	
	##columns to drop
	needs_killing = ['colors', 'count', 'descriptions', 'dimension', 'ean', 
			 'features', 'keys', 'manufacturerNumber', 'quantities', 'reviews', 
			 'skus', 'upc', 'weight', 'prices.availability', 'prices.color', 
			 'prices.returnPolicy', 'prices.shipping', 'prices.warranty', 'prices.offer', 'prices.size',
			 'prices.sourceURLs', 'asins', 'dateAdded', 'imageURLs', 'prices.dateAdded',
			 'prices.isSale', 'prices.amountMax', 'prices.flavor', 'prices.count', 'manufacturer',
			 'merchants']
	for attribute in needs_killing:
		if attribute in maindf.columns:
			maindf = maindf.drop(attribute, axis=1)
#	print 'maindf, after dropping columns: ' + str(maindf.shape)
	
	##ROW ADJUSTMENTS
	#no ebay
	maindf = maindf[maindf['sourceURLs'] != 'www.ebay.com']
	#must have "shoes" in categories
	idx3 = maindf.apply(ACTUALLY_shoes, axis=1)
	maindf = maindf[idx3]
	
	#no condition seems to mean new
	
	return maindf

if __name__ == '__main__':
	outputdf = pd.DataFrame()
	for jsonfile in glob.glob('input_json/*.json'):
		print 'currently working on: ' + jsonfile
		currdf = json_input_to_dataframe(jsonfile)
		print jsonfile + ' final shape: ' + str(currdf.shape)
		outputdf = outputdf.append(currdf, ignore_index=True)
		print 'outputdf shape: ' + str(outputdf.shape)
#	outputdf = json_input_to_dataframe('7243_1.txt.json')
	print "we did it"
	data_frame_to_csv_and_json(outputdf)
