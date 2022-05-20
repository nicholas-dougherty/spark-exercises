# <a name="top"></a>Zillow® Project - README.md
![](http://zillow.mediaroom.com/image/Zillow_Wordmark_Blue_RGB.jpg)

[[Data Dictionary](#dictionary)]
[[Project Description](#project_description)]
[[Project Planning](#project_planning)]
[[Project Acquire](#project_acquire)]
[[Project Prepare](#project_prepare)]
[[Project Explore](#project_explore)]
[[Key Findings](#findings)]
[[Data Acquire, Prep, and Exploration](#wrangle)]
[[Statistical Analysis](#stats)]
[[Modeling](#model)]
[[Conclusion](#conclusion)]


## Data Dictonary
<a name="dictionary"></a>
[[Back to top](#top)]

| Attribute | Definition | Data Type |
| ----- | ----- | ----- |
| taxvaluedollarcnt | The total tax assessed value of the parcel | float |
| bathroomcnt | Number of bathrooms in home including fractional bathrooms | float |
| bedroomcnt | Number of bedrooms in home | float |
| propertylandusetypeid |   Used for joining and determining single family residence | int |
| county |   County in which the property is located) | int |
| fips |   Federal Information Processing Standard code -  see https://en.wikipedia.org/wiki/FIPS_county_code for more details | int |
| age | year_built minus 2017 | int |
| transactiondate |  Day the property was purchased; used for joining | int |
| latitude | Latitude of the middle of the parcel multiplied by 10<sup>6</sup> | float |
| longitude | Longitude of the middle of the parcel multiplied by 10<sup>6</sup> | float |
| parcelid | Unique identifier for parcels (lots) | Index/int | 
| calculatedfinishedsquarefeet | Calculated total finished living area of the home | float |
| taxamount	|  The total property tax assessed for that assessment year | int |
| landtaxvaluedollarcnt |   Amount taxed on the land itself | float |
| logerror |  Calculation of how incorrect zestimate was | float |
| taxrate | taxvaluedollarcnt divided by tax amount | float
| structuretaxvaluedollarcnt | Amount taxed on the finished, livable property | float
| fiscal_quarter | transaction date binned into fiscal quarters | category

## Project Description and Goals
<a name="project_description"></a>

- Project description:
    - Must predict the logerror pertaining to values of single unit properties that the Greater Los Angeles Metropolitan Area tax district assesses, by using property data from parcels which have a transaction in the first three quarters of 2017. 
        - Determine location using the geospatial information garnered through acquisition
        - Data retrieved from Codeup's MySQL database using a query contained in pd.read_sql.

- Goal:
    - Predict the logerror by using Machine Learning and careful Feature Selection
    - Implement clustering and compact the area to heighten accuracy 
    - Reduce capital loss at Zillow through modeling.
    - Prepare a presentation to guides executives through the anomalous attributes within the widely-varied sample of Californian homes in Ventura, Orange and Los Angeles.


# Project Planning
## <a name="project_planning"></a>
[[Back to top](#top)]

 **Plan** -> Acquire -> Prepare -> Explore -> Model & Evaluate -> Deliver

- Tasking out how I plan to work through the pipeline.

### Target variable
- logerror

### Starting focus features
- taxrate
- landcostpersqft
- structurecostpersqft

### For Second Run Through
- age
- bedrooms
- bathrooms
- calculatedfinishedsqft
- fiscal quarters
- county

### Project Outline:
- Acquisiton via Codeup Database
- Preparation and pre-preocessing data using Pandas
    - Remove features
        - too many nulls?
        - not helpful to the quest?
    - Create features as needed
    - Handle null values
        - are the fixable or should they just be deleted
    - Handle outliers
    - Split Data before EDA
- Exploratory Data Analysis
     - Visualization using MatPlotLib and Seaborn
- Statistical Testing
- Modeling: Primary focus is Regression in its many forms 
- Implementation via Test set
- Conclude results

### Hypotheses
- The costs per square feet for property, when contained in clusters based on Latitude and Longitude, will help elucidate the logerror of the Zestimate.
- Taxrates, when noticably higher, impact this error as well
- The age of property in Orange, given its inclusion of older estates (which may not be in this dataframe, but are certainly aspects of the county itself) will distort and lead to error as well
- Greater area will not always indicate higher value (and higher error) but is a good lead. 

# Project Acquisition
<a name="project_acquire"></a>
[[Back to top](#top)]

 Plan -> **Acquire** -> Prepare -> Explore -> Model & Evaluate -> Deliver

Functions used can be found in wrangle.py in git hub repo

1. acquire the zillow data from the codeup sequel server and convert it into a pandas df
    `def acquire_zillow_data(use_cache=True):
    '''
    This function returns a snippet of zillow's database as a Pandas DataFrame. 
    When this SQL data is cached and extant in the os directory path, return the data as read into a df. 
    If csv is unavailable, aquisition proceeds regardless,
    reading the queried database elements into a dataframe, creating a cached csv file
    and lastly returning the dataframe for some sweet data science perusal.
    '''

    # If the cached parameter is True, read the csv file on disk in the same folder as this file 
    if os.path.exists('zillow.csv') and use_cache:
        print('Using cached CSV')
        return pd.read_csv('zillow.csv', dtype={'buildingclassdesc': 'str', 'propertyzoningdesc': 'str'})

    # When there's no cached csv, read the following query from Codeup's SQL database.
    print('CSV not detected.')
    print('Acquiring data from SQL database instead.')
    df = pd.read_sql(
        '''
 SELECT
    prop.*,
    predictions_2017.logerror,
    predictions_2017.transactiondate,
    air.airconditioningdesc,
    arch.architecturalstyledesc,
    build.buildingclassdesc,
    heat.heatingorsystemdesc,
    landuse.propertylandusedesc,
    story.storydesc,
    construct.typeconstructiondesc
FROM properties_2017 prop
JOIN (
    SELECT parcelid, MAX(transactiondate) AS max_transactiondate
    FROM predictions_2017
    GROUP BY parcelid
) pred USING(parcelid)
JOIN predictions_2017 ON pred.parcelid = predictions_2017.parcelid
                      AND pred.max_transactiondate = predictions_2017.transactiondate
LEFT JOIN airconditioningtype air USING (airconditioningtypeid)
LEFT JOIN architecturalstyletype arch USING (architecturalstyletypeid)
LEFT JOIN buildingclasstype build USING (buildingclasstypeid)
LEFT JOIN heatingorsystemtype heat USING (heatingorsystemtypeid)
LEFT JOIN propertylandusetype landuse USING (propertylandusetypeid)
LEFT JOIN storytype story USING (storytypeid)
LEFT JOIN typeconstructiontype construct USING (typeconstructiontypeid)
WHERE prop.latitude IS NOT NULL
  AND prop.longitude IS NOT NULL
  AND transactiondate <= '2017-12-31';             
        '''
                    , get_db_url('zillow'))
    
    df.propertyzoningdesc.astype(str)
    
    
    print('Acquisition Complete. Dataframe available and is now cached for future use.')
    # create a csv of the dataframe for the sake of efficiency. 
    df.to_csv('zillow.csv', index=False)
    
    return df
`

2. check out the .info
    - Takeways
        - There are 34 columns missing >= 50% of their values. 
            - These will be dropped. 
3. Used UDF describe data to closely inspect the contents.


# Project Preperation
<a name="project_prepare"></a>
[[Back to top](#top)]

 Plan -> Acquire -> **Prepare** -> Explore -> Model & Evaluate -> Deliver

Functions used can be found in wrangle.py in git hub repo

1. clean the data:
    ```def prep_zillow_og(df):
    '''
    The full preparation cycle outlined by print-statements
    '''
    
    print('Beginning preparation...\n')
    print('Detecting Nulls; set to delete columns and then rows are comprised of 50% nulls')      
    df = data_prep(df)
    
    print('''
    The following 34 columns were dropped because they were missing more than 50.0% of data: 
['airconditioningtypeid', 'architecturalstyletypeid', 'basementsqft', 'buildingclasstypeid', 'decktypeid', 'finishedfloor1squarefeet', 'finishedsquarefeet13', 'finishedsquarefeet15', 'finishedsquarefeet50', 'finishedsquarefeet6', 'fireplacecnt', 'garagecarcnt', 'garagetotalsqft', 'hashottuborspa', 'poolcnt', 'poolsizesum', 'pooltypeid10', 'pooltypeid2', 'pooltypeid7', 'regionidneighborhood', 'storytypeid', 'threequarterbathnbr', 'typeconstructiontypeid', 'yardbuildingsqft17', 'yardbuildingsqft26', 'numberofstories', 'fireplaceflag', 'taxdelinquencyflag', 'taxdelinquencyyear', 'airconditioningdesc', 'architecturalstyledesc', 'buildingclassdesc', 'storydesc', 'typeconstructiondesc']
0 rows were dropped because they were missing more than 50.0% of data''')
    
    print('Selecting propertylanduse by the potential that they classify as single family residential, although indirectly\n')
    df = df[(df.propertylandusedesc == 'Single Family Residential') |
      (df.propertylandusedesc == 'Mobile Home') |
      (df.propertylandusedesc == 'Manufactured, Modular, Prefabricated Homes') |
      (df.propertylandusedesc == 'Cluster Home')]
    print('Plausible candidates: Mobile Home; Manufactured, Modular, Prefabricated Homes; Cluster Home; Single Family Residential\n')
    
    print('Removing properties that have zero bathrooms and zero bedrooms\n')
    # Remove properties that couldn't even plausibly be a studio. 
    df= df[(df.bedroomcnt > 0) & (df.bathroomcnt > 0)]
    # Remove properties where there is not a single bathroom.
    df = df[df.bathroomcnt > 0]
    
    print('Preserving properties with a finished living area greater than 70 square feet\n')
 # keep only properties with square footage greater than 70 (legal size of a bedroom)
    df = df[df.calculatedfinishedsquarefeet > 70]
    print('California housing standards classify a bedroom as being greater than 70 sqft, any properties beneath this are undeniably not homes\n')
    
    print('The minimum lot size of single family units is 5000 square feet. DataFrame excludes fields that fail to meet this criteria.\n')
    # Minimum lot size of single family units.
    df = df[df.lotsizesquarefeet >= 5000].copy()

    # Clear indicators of single unit family. Other codes non-existent or indicate commercial sites. 
   # 0100 - Single Residence
   # 0101 Single residence with pool
   # 0104 - Single resident with therapy pool 
    print('Preserving certain property county landuse codes based on research\n')
    df = df[(df.propertycountylandusecode == '0100') |
            (df.propertycountylandusecode == '0101') |
            (df.propertycountylandusecode == '0104') |
            (df.propertycountylandusecode == '122') | 
            (df.propertycountylandusecode == '1111') |
            (df.propertycountylandusecode == '1110') |
            (df.propertycountylandusecode == '1')
           ]
    
    print('Removed 13 rows where unit count is 2. Duplexes are not permitted. After reasoning through remainders, Orange county had many false detailed Nulls that were actually HIGHLY LIKELY single family residences. Unit Count NA filled with 1.\n')
    # Remove 13 rows where unit count is 2. The NaN's can be safely assumed as 1 and were just mislabeled in other counties.  
    df = df[df['unitcnt'] != 2]
    df['unitcnt'].fillna(1)
    
    
    # Property where finished area is 152 but bed count is 5. 
    df = df.drop(labels=75325, axis=0)
    
      
            
    # Redudant columns or uninterpretable columns
    # Unit count was dropped because now its known that theyre all 1. 
    # Finished square feet is equal to calculated sq feet. 
    # full bathcnt and calculatedbathnbr are equal to bathroomcnt
    # property zoning desc is unreadable. 
    # assessment year is unnecessary, all values are 2016. 
    # property land use desc is always single family residence 
    # same with property landuse type id. 
    # room count must be for a different category, as it is always 0.
    # regionidcounty reveals the same information as FIPS. 
    # heatingorsystemtypeid is redundant. Encoded descr. 
    # Id does nothing, and parcelid is easier to represent. 
    print('Dropping columns that are redundant or provide no discernible value to this set. Details included in wrangle.py \n')
    
    df =df.drop(columns= ['finishedsquarefeet12', 'fullbathcnt', 'calculatedbathnbr',
                      'propertyzoningdesc', 'unitcnt', 'propertylandusedesc',
                      'assessmentyear', 'roomcnt', 'regionidcounty', 'propertylandusetypeid',
                      'heatingorsystemtypeid', 'id', 'heatingorsystemdesc', 'buildingqualitytypeid',
                         'rawcensustractandblock'],
            axis=1)
    
    print('Remaining Null Count miniscule. Less than 0.000% of DF. Dropping values. \n')
    # The last nulls can be dropped altogether. 
    df = df.dropna()
    
    print('City and Zip Code must have five place-holders. Many do not, are were dropped \n')
    # the city code is supposed to have five digits. Converted to integer to do an accurate length count as a subsequent string. 
    df.regionidcity = df.regionidcity.astype(int)
    df = df[df.regionidcity.astype(str).apply(len) == 5]
    
    # the same applies to the zip code. 
    
    df.regionidzip = df.regionidzip.astype(int)
    df = df[df.regionidzip.astype(str).apply(len) == 5]
    
    print('Calculating and creating an age column')
    df['yearbuilt'] = df['yearbuilt'].astype(int)
    df.yearbuilt = df.yearbuilt.astype(object) 
    df['age'] = 2017-df['yearbuilt']
    df = df.drop(columns='yearbuilt')
    df['age'] = df['age'].astype('int')
    print('Yearbuilt converted to age. \n')
    
    print('County column created using Federal Information Processing Standards')
    df['county'] = df.fips.apply(lambda fips: '0' + str(int(fips)))
    df['county'].replace({'06037': 'los_angeles', '06059': 'orange', '06111': 'ventura'}, inplace=True)
    
    print('Creating columns for tax rate and cost per square foot for structure and land \n')
    # Feature Engineering
     # create taxrate variable
    df['taxrate'] = round(df.taxamount/df.taxvaluedollarcnt*100, 2)
    # dollar per square foot- structure
    df['structure_cost_per_sqft'] = df.structuretaxvaluedollarcnt/df.calculatedfinishedsquarefeet
    # dollar per square foot- land
    df['land_cost_per_sqft'] = df.landtaxvaluedollarcnt/df.lotsizesquarefeet
    
    print('Using k=3, removing outliers from continuous variables')
    df = remove_outliers(df, 3, ['lotsizesquarefeet', 'structuretaxvaluedollarcnt', 'taxvaluedollarcnt',
                                'landtaxvaluedollarcnt', 'taxamount', 'calculatedfinishedsquarefeet', 'structure_cost_per_sqft',
                                'taxrate', 'land_cost_per_sqft', 'bedroomcnt', 'bathroomcnt'])
    print('Creating fiscal quarters according to transaction date \n')
    # create quarters based on transaction date
    # first convert from string to datetime format
    df['transactiondate'] = pd.to_datetime(df['transactiondate'], infer_datetime_format=True, errors='coerce')
    # then use pandas feature dt.
    df['fiscal_quarter'] = df['transactiondate'].dt.quarter
    # drop transaction date, since it can't be represented in a histogram 
    # actual dates can be retrieved from parcelid for those interested
    df = df.drop(columns='transactiondate')
    print('As per Californian tax lexy standard, taxrate listed as less than 1% removed from dataframe. Error either on input or tax record itself.')
    # lastly, even after removing outliers from those columns, a few tax rates under 
    # 1% are present. This is unacceptable, as the Maximum Levy (in other words the 
    # bare minimum, too) is 1%. Additional fees can be added, but there's no getting 
    # under 1%. thus, rows falling beneath this must go. 
    df = df[df.taxrate >= 1.0]
    
    # move decimal points so lat
    # and long are correct. 
    print('Moving decimal points 6 to the left for latitude and longitude, as is necessary to capture the appropriate coordinates for this area of interest. \n')
    lats = df['latitude']
    longs = df['longitude']
    
    round(moveDecimalPoint(lats, -6), 6)
    round(moveDecimalPoint(longs, -6), 6)
    
    print('Setting parcelid as the index. \n')
    #finally set the index
    df = df.set_index('parcelid')
    
        # A row where the censustractandblock was out of range. Wasn't close to the raw, unlike the others, and started with 483 instead of 60, 61. Too large. 
    df = df.drop(labels=12414696, axis=0)
    
    print('\n\n\nPreparation Complete. Begin Exploratory Data Analysis.')
    return df```

# Project Exploration
<a name="project_explore"></a>
[[Back to top](#top)]

 Plan -> Acquire -> Prepare -> **Explore** -> Model & Evaluate -> Deliver

1. Separate train and use only that df for EDA.
2. Correlation heatmaps demonstrate limited correlation among the variables from the outset
    - most likely due to lack of scaling.
    - Don't wish to scale Latitude and Longitude before playing with their clusters
    - General consensus is that doing so would rob it of its rich value. 
3. Bi-variate and Multivariate visualizations via Seaborn and Mito. 


# Stat Tests
## <a name="stats"></a>Statistical Analysis
[[Back to top](#top)]

Correlation Test
 - Used to check if two samples are related. They are often used for feature selection and multivariate analysis in data preprocessing and exploration.

