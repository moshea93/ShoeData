import pandas as pd
import string

def keep_currencies(currency):
	if currency == 'USD':
		return True
	return False

def keep_conditions(condition):
	if 'new' in condition.lower():
		return True
	if condition == '':
		return True
	else:
		return False

def keep_prices(price):
	if price < 100000:
		return True
	return False

def keep_brands(brand):
	kill_brands = ['blue', 'no brand', 'carter\'s', 'circo', 'disney', 'heelys', 'tundra']
	if brand in kill_brands:
		return False
	return True

def get_gender(row):
	categories = row['categories']
	name = row['name'].lower().strip()
	women_keywords = ['women\'s', 'women', 'girls']
	men_keywords = ['men\'s', 'boys', 'mens ', ' mens']
	women = False
	men = False
	for category in categories:
		category = category.lower()
		for keyword in women_keywords:
			if keyword in category:
				women = True
				break
		else:
			for keyword in men_keywords:
				if keyword in category:
					men = True
			if category == 'men':
				men = True
	if not men and not women:
		for keyword in women_keywords:
			if keyword in name:
				women = True
				break
		else:
			for keyword in men_keywords:
				if keyword in name:
					men = True
	if men and women:
		return 'both'
	elif women:
		return 'W'
	elif men:
		return 'M'
	else:
		return 'IDK'

def further_adjustments(jsonfile):
	maindf = pd.read_json(jsonfile)
	print 'maindf: ' + str(maindf.shape)
	
	#filtering rows
	maindf = maindf[maindf['prices.currency'].fillna('').apply(keep_currencies)]
	maindf = maindf[maindf['prices.condition'].fillna('').apply(keep_conditions)]
	maindf = maindf[maindf['prices.amountMin'].apply(keep_prices)]
	maindf['gender'] = maindf.apply(get_gender, axis=1)
	maindf['brand'] = maindf['brand'].str.lower()
	maindf = maindf[maindf['brand'].apply(keep_brands)]


	#brand name adjustments
	maindf['brand'] = maindf['brand'].replace({'guiseppe zanotti': 'giuseppe zanotti', 'michael michael-kors': 'michael michael kors', 'toms shoes': 'toms', 'polo by ralph lauren': 'polo ralph lauren','usadawgs': 'usa dawgs', 'mossimo black': 'mossimo supply co'})
	maindf.loc[(maindf['brand'].apply(lambda x: x[:3]) == 'ugg') & (maindf['brand'].apply(lambda x: x[-9:]) == 'australia'), 'brand'] = 'ugg australia'
	maindf.loc[(maindf['brand'].apply(lambda x: x[:5]) == 'asics') & (maindf['brand'].apply(lambda x: len(x)) == 6), 'brand'] = 'asics'	
	maindf.loc[(maindf['brand'].apply(lambda x: x[:5]) == 'crocs') & (maindf['brand'].apply(lambda x: len(x)) == 12), 'brand'] = 'crocs'
	

	#renaming, removing columns
	maindf = maindf.rename(columns={'prices.amountMin': 'price', 'prices.dateSeen': 'dateSeen', 'prices.currency': 'currency'})
	needs_killing = ['prices.condition', 'prices.merchant', 'sourceURLs', 'dateUpdated']
	maindf = maindf.drop(needs_killing, axis=1)

#	maindf = maindf.reset_index()
#	print maindf.head()
	print 'maindf: ' + str(maindf.shape)	
	return maindf

if __name__ == '__main__':
	outputdf = further_adjustments('cleaned_entries.json')
#	subset = outputdf.loc[0:499,:]
#	subset.to_csv('final_sample.csv', encoding='utf-8')
	outputdf.to_csv('final_entries.csv', encoding='utf-8')
	outputdf.to_json('final_entries.json')
