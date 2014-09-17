from pyspark import SparkContext, SparkConf
import sys
import json

conf = SparkConf().setAppName(__name__)
sc = SparkContext(conf=conf)

booksURI = sys.argv[1]
assetsURI = sys.argv[2]
outputURI = sys.argv[3]

# create an RDD of all the books that have a print_isbn
books = sc.textFile(booksURI).map(json.loads).filter(
    lambda book: (
        "base_metadata" in book 
        and 'print_isbn' in book['base_metadata'] 
        and book['base_metadata']['print_isbn'] is not None
   )
)

# create an RDD of all the assets with isbns
assets = sc.textFile(assetsURI).map(json.loads).filter(
    lambda asset: (
        "isbn" in asset and asset['isbn'] is not None
        and "asset_id" in asset and asset['asset_id'] is not None
    )
)

# key each book by isbn and join it to the assets keyed on isbn 
joined = books.map(
    lambda b: (b['base_metadata']['print_isbn'], b)
).join(
    assets.map(lambda a: (a['isbn'], a))
)

# add the asset_id to the base_metadata of each book
def setAssetId(book, asset_id):
    book['asset_id'] = asset_id
    return book

booksWithAssetIds = joined.map(
        lambda (k, (b, a)): setAssetId(b, a['asset_id'])
)

# Store the result as a text file (this would realistically go into a service database for answering queries)
booksWithAssetIds.map(lambda b: json.dumps(b)).saveAsTextFile(outputURI)
