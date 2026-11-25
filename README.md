Market-Basket Analysis on the IMDb Dataset

1. **Project Overview**

This project applies market-basket analysis to the public IMDb dataset to discover frequent itemsets and association rules. The analogy is:

Market-basket concept	IMDb mapping
Basketball (transaction)	A movie / title
Also	An actor or actress in that movie
Frequent itemset	A group of actors that appear together in many titles
Association rule X => Y	"If actors X appear in a title, actor Y very likely appears too"

Two algorithms are implemented and compared on Apache Spark (PySpark):

FP-Growth (Spark MLlib)
A-Priori (custom implementation using RDDs)

2. **Dataset**

Source: the IMDb dataset on Kaggle (). Files are gzipped TSVs; marks missing values.ashirwadsangwan/imdb-dataset\N

File	Content	Key columns
title.akas.tsv.gz	Alternative / localized titles	titleId, , , , , , , orderingtitleregionlanguagetypesattributesisOriginalTitle
title.basics.tsv.gz	Core title information	tconst, , , , , , , , titleTypeprimaryTitleoriginalTitleisAdultstartYearendYearruntimeMinutesgenres
title.principals.tsv.gz	Principal cast and crew by title	tconst, , , , , orderingnconstcategoryjobcharacters
title.ratings.tsv.gz	Ratings	tconst, , averageRatingnumVotes
name.basics.tsv.gz	People	nconst, , , , , primaryNamebirthYeardeathYearprimaryProfessionknownForTitles

Files actually used for the analysis: , , . and are described in the report but are not needed for the basket construction.title.principalsname.basicstitle.akastitle.basicstitle.ratings

3. **Pipeline**
   
3.1 Data organization
Data is loaded with the Spark DataFrame API (named columns, equivalent to relational tables) and processed with Spark SQL.
The A-Priori implementation uses PySpark RDDs.

3.2 Pre-processing
Load , , .title.principalsname.basicstitle.akas
Filter to the categories actor and actress.title.principals
Join with on to obtain actor names.name.basicsnconst
Use and to produce one row per element, enabling joins.split()explode()
Join with to attach movie titles.title.akas
Group by movie title so that each title becomes one basket containing a list of actor names.

3.3 **Algorithms**
FP-Growth
Pass 1 counts item frequencies and keeps the frequent items.
Pass 2 encodes transactions in an FP-tree (suffix-tree structure), so no candidate sets are generated explicitly; frequent itemsets are mined from the tree.
Spark parameters:
minSupport: minimum fraction of baskets containing the itemset (e.g. 3/5 = 0.6).
minConfidence: minimum confidence for rules (e.g. X in 4 baskets, X and Y together in 2, so confidence = 0.5).
numPartitions: parallelism (defaults to the input's partition count).
Model outputs: , , (applies rules to a transaction and predicts consequents).freqItemsetsassociationRulestransform
a priori
Step 1: map item names to integers and count occurrences in an array.
Between passes: keep items with count >= threshold s and build the frequent-items table.
Step 2: count only pairs whose members are both frequent (monotonicity: a pair cannot be frequent unless both items are).
Generalization: one pass per items and size k. = candidates of size k, = truly frequent itemsets of size k. Stop when is empty.C_kL_kL_k

4. **Experiments**
FP-Growth
minSupport = 0.0001, (chosen empirically as "better results").minConfidence = 0.001
Output: frequent itemsets and association rules (antecedent, consequent, confidence, lift, support).
A-Priori (RDD implementation)
Data is parallelised into a distributed dataset.
supportRDD computes single-item support; accumulates support tables.baseRDD
min_support = 2 (absolute count): itemsets appearing in fewer than 2 baskets are discarded.
Candidate generation uses +; frequencies are computed with and filtered by the minimum support.cartesiandistinctreduceByKey
New support tables are added to , is updated, and finally confidence is computed and filtered.baseRDDsupportRDD

5. **Results**

The report shows a table of FP-Growth rules. Notes:

Every displayed rule has confidence = 1.0.
Lift values are very large (roughly 6,000 to 8,000 for the first group, visible in the table).
Support values are very small (on the order of 1e-4, consistent with ; the last column is truncated in the PDF screenshot).minSupport = 0.0001
The rules cluster into groups of people who clearly belong to the same production. For example, one cluster lists Stephen Hunter, William Kircher, Sylvester McCoy, Dean O'Gorman, Graham McTavish, Ken Stott, Aidan Turner, Adam Brown, Peter Hambleton and Richard Armitage, which appears to be the cast of The Hobbit films. Another cluster (Michael Haigney, John Loeffler, Rica Matsumoto, Yūji Ueda, Mayumi Izuka, ...) looks like an anime/dub cast. (Interpretation by the analysis, not stated in the report.)

Authors' conclusion: FP-Growth yields rules at very low support, whereas A-Priori could not produce rules at low support. They attribute this to the dataset having very low similarity between baskets.

6. **Critical Analysis**
   
**Strengths**
Clear and sensible modelling of the problem (movies = baskets, actors = items).
Comparison of a library implementation (FP-Growth) with a from-scratch one (A-Priori).
Use of Spark makes the approach horizontally scalable.
The rules found are interpretable and make sense (co-starring casts).

**Why FP-Growth outperforms A-Priori here**
With , the number of candidate itemsets A-Priori must generate and count is huge, and joins explode quadratically or worse.minSupport = 0.0001cartesian
FP-Growth avoids candidate generation and needs only two scans of the data.
The data is extremely sparse: millions of actors, only a handful per movie, so few itemsets are frequent and the FP-tree compresses well.

**Weaknesses and issues to be aware of**
1. Trivial rules. Confidence = 1.0 with huge lift means the actors co-occur in essentially one production (or a small series). The rules describe "same cast of the same franchise" rather than non-obvious associations. Possible improvements: dedupe episodes/versions of the same title, restrict to feature films, or filter by .numVotes
2. Grouping by title. Basket identity should be the title ID (), not the title string. Different movies share the same title, and contains many localised titles per movie, which can duplicate or merge baskets incorrectly.tconsttitle.akas
Inconsistent support definitions. FP-Growth uses a relative threshold (0.0001) while A-Priori uses an absolute count (2). The two results are therefore not directly comparable.
3. No quantitative comparison. The report gives no runtimes, number of baskets, number of frequent itemsets, or memory usage, so the claims about scalability and performance are not backed by measurements.
4. A-Priori failure is not demonstrated. The statement that A-Priori "could not provide confidence for low values" lacks supporting output, such as the number of candidates per level or timing/OOM evidence.
5. Scalability section is generic. It describes Spark (DAG scheduler, Catalyst, RDDs) rather than measuring the project's scaling. The claim that the "default datatype is String, which occupies relatively less memory" is questionable, and "regular expressions" are mentioned but not used in the described pipeline.
6. Data description errors. is documented as "0 - Adult, 1 - not Adult", which is reversed (0 = non-adult, 1 = adult in IMDb). should be . includes up to three genres.isAdultaverageRatingsaverageRatinggenres
7. Pre-processing wording. The explanation of / on "primary names of the actor" is unclear; the order of operations (group, then split, then explode) is not fully specified.split()explode()
8. Smaller types. "priori", "supportrdd/supportRDD", "Inorder", "creat", "detector of similar items" (leftover from a different project template).
9. Reproducibility. No code, Spark version, cluster configuration, or sampling description is included.

7. **Suggested Repository Structure**
.
├── README.md
├── report/
│   └── Market_basket_IMDb.pdf
├── notebooks/
│   ├── 01_preprocessing.ipynb
│   ├── 02_fpgrowth.ipynb
│   └── 03_apriori_rdd.ipynb
├── data/            # not committed; download from Kaggle
└── requirements.txt

8. **How to Run (outline)**

1. Download the IMDb TSV files from Kaggle and place them in .data/
2. Install dependencies: .pip install pyspark
3. Run the pre-processing notebook to build baskets (one row per title, array of actor names).
4. Run FP-Growth:
python
from pyspark.ML.fpm import FPGrowth fp = FPGrowth(itemsCol="actors", minSupport=0.0001, minConfidence=0.001) model = fp.fit(baskets) model.freqItemsets.show() model.associationRules.show()
5. Run the A-Priori RDD notebook with .min_support = 2
