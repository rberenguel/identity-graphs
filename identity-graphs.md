footer: ![](images/hybrid.webp)
[.footer-style: #2F2F2F, alignment(left), line-height(1), text-scale(3.5), z-index(10000)]

---

[.hide-footer]

![fit](images/dais-title.jpg)

^ ...

---

[.footer: ![left](images/hybrid-red.webp)]

![](Images/RubenBerenguel.jpg)
![](Images/hybrid-vert.webp)
![](Images/scala.webp)
![](Images/python.webp)


# whoami

- Ruben Berenguel ([_@berenguel_](http://twitter.com/berenguel))
- PhD in **Mathematics**
- Lead Data Engineer at [**Hybrid Theory**](http://hybridtheory.com)
- Preferred stack is **Python**, **Go** and **Scala**

---

|||
| --- | :--- |
|Part 1 | **Set up**|
|Part 2 | **The identity graph**|
|Part 3 | **Speed up** and **improvements**|
|  |  |

^ I have divided this presentation in 3 sections: first I will talk about adtech, cookies and the identity problem. Then I will explain how we can solve the problem using an identity graph, and finally how we can process this graph fast with Apache Spark

---

| Part 1: **Set up** |
| --- |
| _Adtech_ |
| **_What are cookies, really?_** |
| _What is cookie mapping?_ |
| _**The identity problem**_ |

^ I will start by setting up the problem: how programmatic advertising uses cookie to create targeted ads, and the user-identity problem that derives from it

---

[.hide-footer]
[.build-lists: true]

![left](images/newspapers.jpg)


# Programmatic adtech

## _Find users satisfying some **criteria**_

- **Visited pages** of category `ABC`
- **Are interested** in concept `XYZ`
- Are **likely to want to buy** from our client `RST`

^ Note that the third bullet needs some kind of machine learning or using very smart humans. The final goal is that if you are going to see an ad, it better is a relevant one

---

![right](images/loupe.jpg)

## To find them we need

### Their **browse** and/or **behaviour data** 🖥 🛒 

## 　
### 　

---

![right](images/loupe.jpg)

## _To find them we need_

### _Their **browse** and/or **behaviour data** 🖥 🛒_

## To deliver for our clients we need

### A way to show them **ads** 📣

^ All boils down to identifying specific users online. But what identifies a user online, so we can show an ad only to someone who is going to be interested in it? That's cookies 🍪🥠

---

![left](images/Cookie.jpg)

[.hide-footer]

# **Cookies**

## Are used to help websites　　 
## **track events** 
## and **state**
## as users browse

^ Note that most cookies are going away in the next years due to privacy concerns and regulations.

---

![right](images/party.jpg)

## There are _two kind_ of cookies
### **First** party (session, state…)
####
### **Third** party (event tracking…)

^ Session/state would be your basket, whether you are logged in, etc. Event tracking ranges from advertising (or related) stuff to analytics (like Google)

---

![fit](images/Cookies-1.webp)

^ A user is browsing online

---

![fit](images/Cookies-2.webp)

^ A _first party webserver_ is serving a webpage

---

![fit](images/Cookies-3.webp)

^ A __third party webserver_ is serving a _pixel_ on the page

---

![fit](images/Cookies-4.webp)

^ This server sets a cookie on the user/browser combination

---

![fit](images/Cookies-5.webp)

^ Stamp!

---

![fit](images/Cookies-6.webp)

^ When the user goes to the login area…

---

![fit](images/Cookies-7.webp)

^ The first party server keeps track of that _state_ by setting a cookie on the user/browser

---

![fit](images/Cookies-8.webp)

^ Stamp!s

---

![fit](images/Cookies-9.webp)

^ Cookies are associated to the domain that set them, and they are not accessible from others. So, the first party server knows nothing about the third party cookie (and conversely)

---

![fit](images/Cookies-10.webp)

---

### We get **browse data** from users on the web from data providers[^A]
## 　

[^A]: Event logs with cookies provided in batch by **data providers**

^ So, in the end we get large amounts of batch data of what users do

---

### _We get **browse data** from users in the web from data providers_
### We get **browse data** from users browsing our client website[^B]

[^B]: Event logs with cookies generated from our servers, via **our pixels**

^ And data from what users do on our clients websites, which we handle with our third party 🍪

---

![left](images/socket.jpg)

## How do we **connect** both **data sources**? 

## _The identifiers we get from both sides are unrelated!_

## 😱

^ Those wall sockets look scared

---

![right](images/map.jpg)

## Mapping _servers_ 
## and
## the _mapping chain_

^ For advertising to be effective, we need to connect these two data sources, what happens on our clients' websites and what happens around the world

---

![fit](images/Mapping-1.webp)

^ In cookie mapping, a user is browsing

---

![fit](images/Mapping-2.webp)

^ A pixel fires, a cookie is set

---

![fit](images/Mapping-3.webp)

---

![fit](images/Mapping-4.webp)



---

![fit](images/Mapping-5.webp)

^ The destination server (mapping server) redirects to another server

---

![fit](images/Mapping-6.webp)

^ that sets a cookie

---

![fit](images/Mapping-7.webp)


---

![fit](images/Mapping-8.webp)

^ and calls back to the initial server, reporting back the identifier that has been set in the cookie

---

![fit](images/Mapping-9.webp)

^ This can repeat any number of times (although less is better)

---

![fit](images/Mapping-10.webp)

---

![fit](images/Mapping-11.webp)

---

![fit](images/Mapping-12.webp)

---

![fit](images/Mapping-13.webp)

^ This is what the chain looks like then: a chain of identifiers (or cookies) that are tied to a user. This is as seen from the server initiating the redirections

---

![left](images/mask.jpg)

[.hide-footer]

# The identity problem

^ 🥸 Greetings, good man. Might I trouble you for a drink? Homer? Who is Homer?

---

![fit](images/Chains.webp)

^ These are a few chains. A virtual id for each chain is added when the redirections start and keep track of the callbacks. The identity problem appears when you try to keep the chains up to date as days pass: some cookies degrade fast, and a user may have several identifiers for each partner. Handling this adhoc results in mapping issues

---

[.build-lists: true]

# Basic solution
- Coalesce (_merge on nulls_) chains based on *one id*
- Is not as complete as the _graph approach_ because…
- Requires one stable identifier

^ (Or stable enough identifier). This solution can be applied to batches of chains without requiring any lookback. The coalescing either kills other identifiers (and requires a stable identifier) or results in an overwrite of identifiers

---

![fit](images/Connected-1.webp)

^ What do we do in this situation? Either `id 77` goes with circle or with gamma. Unless…

---

|Part 2: **The identity graph** |
| --- |
| _Rethink the problem as a graph_ |
| _**Connected components in big data**_ |

---

![fit](images/Break-chains-1.webp)

^ Recall the table with chains

---

![fit](images/Edges-1.webp)

^ Think them as nodes in graphs

---

![fit](images/Edges-2.webp)

^ Remove useless info

---

![fit](images/Edges-3.webp)

^ Three connected components, three users

---

![fit](images/Edges-4.webp)

^ Ignore useless sources that add no information

---

![fit](images/Edges-5.webp)

^ This new information goes here

---

![fit](images/Edges-6.webp)

^ And here

---

![fit](images/Edges-7.webp)

^ With a coalescing solution, you would have 4 users, or best case scenario the system would resolve that user 42 is one o user 2 or user gamma.

---

![fit](images/Edges-6.webp)

---

![fit](images/Edges-8.webp)

^ By looking for connected components you realise there are actually 2 users instead of 3. How do we find connected components with Spark?

---

## _Enter_ `GraphFrames`

---

[.hide-footer]

![left](images/web.jpg)

## Basic Spark graph framework: **GraphX**

### It is message-propagation[^C], graph-parallel, **low level**

#### 　


[^C]: Like the Pregel API

^ The Pregel "message passing model" is very handy in its flexibility. It allows to create non-deterministic identity graphs as well, like the graph you could create to figure out cross-device identities (since cookies are set per browser)

---

[.hide-footer]

![left](images/web.jpg)

## _Basic Spark graph framework: **GraphX**_

### _It is message-propagation (Pregel API) graph-parallel, **low level**_

### **GraphFrames** are to **DataFrames** as **GraphX** is to **RDD**s

^ But a higher level API is more convenient

---

## Alternatives considered…

|||
|:---|:---|
| **Apache Giraph** | harder maintenance |
| **Neo4J** | harder scalability |
| **AWS Neptune** | too new |

^ Except Giraphe, most options available are _graph databases_ and not _graph computation engines_. The difference is important for our problem: we want to find connected components, not query. Graph databases are optimised for querying (and offer custom languages for it, like Gremlin)

---

### Input should be formatted as a `DataFrame` of edges

| src | dst | (…) |
| --- | --- | --- |
| _partner\_1\__**𝟷** | _partner\_2\__**⍺** | 1617963647… |
| _partner\_1\__**2** | _partner\_3\__**⭘** | 1617963647… |
| _partner\_2\__**𝛄** | _partner\_3\__**△** | 1617963654… |
| ⁞ | ⁞ | ⁞ |

^ We can additionally pass any information related with an edge (generically call it _label_), most useful would be the timestamp of the event.

---

![right](images/bubbles.jpg)

## **Connected components in big data**
## The _Large Star - Small Star_ algorithm

^ The algorithm converts each connected component in a star (a cartwheel). There are several alternative algorithms that improve on large star - small star, like union-find-shuffle and partition-aware connected components

---

![fit](images/LS-SS-1.webp)

^ Start with a graph, directed or undirected

---

![fit](images/LS-SS-2.webp)

^ Randomly assign a different integer to all nodes. In GraphFrames this is done by adding a monotinically-increasing id to each node. Next step is a preparation step for humans, as the first step in large star

---

![fit](images/Large-Star-1.webp)

^ First start with the Large Star step. This step is done for the local neighborhood of each node. To make it clearer, let's point from _large_ to small first.

---

![fit](images/Large-Star-2.webp)

^ The large star step is done per node, where we need to consider the immediate neighborhood. For example, let's check node 7

---

![fit](images/Large-Star-3.webp)

^ It has two neighbors, 10 and 3. In this step, we connect all strictly larger neighbors (including _self_) to the minimum neighbor

---

![fit](images/Large-Star-4.webp)

^ I.e. we connect 10 and 7 itself to 3

---

![fit](images/Large-Star-5.webp)

^ This is done to all nodes. You can imagine water flowing down the slopes. 3 doesn't go to 1 because it's smaller than 9 for example.

---

![fit](images/Small-Star-1.webp)

^ After the large star step, we come to the small star step. 

---

![fit](images/Small-Star-2.webp)

^ This is again a node-local algorithm. Let's focus on node 9 

---

![fit](images/Small-Star-3.webp)

^ and its neighbors.

---

![fit](images/Small-Star-4.webp)

^ In this case, we need to connect the _strictly smaller_ neighbors (including _self_) to the minimal neighbor. In this case, we connect 3 and 9 to 1.

---

![fit](images/Small-Star-5.webp)

^ And likewise for all other nodes and its neighbors (in this case there are no additonal changes). All these node-local processes can be easily computed in a "SQL" way that can be parallelized by Spark

---

![fit](images/Iterate-1.webp)

^ Now we iterate, by applying Large Star again, which will link all neighbors of 3 (and 3) to 1.

---

![fit](images/Iterate-2.webp)

^ And we end up with a star, where all nodes are connected to a node with minimal id. We use this id as the connected component id. The algorithm is \mathcal{O}(\log^2\text{number of nodes}), although in practice it is significantly faster, because convergence depends on the height (or diameter) of the worst component. It can have horrible _last reducer_ problems due to very large components in that case.

---

# Output layout

| Component Id | Partner / Cookie Id | Timestamp |
| --- | --- | --- |
| 10234 | _partner\_1\__**𝟷** | 1617963647 |
| 10234 | _partner\_2\__**⍺** | 1617963647 |
| 5534 | _partner\_1\__**2** | 1617963654 |
| ⁞ | ⁞ | ⁞ |

---

[.build-lists: true]

To map from _Partner A_ to _Partner B_

- Given an id _Partner\_A\__**X**,
- we find the **connected component id** for the node _Partner\_A\__**X**,
- we find all the nodes of the form _Partner\_B\____*__ for the component above

---

[.build-lists: true]

# Impact of moving **from an adhoc process to a graph process**

- _**Partner integration**_: from **2 months** to **1 week**
- _**Users mapped uplift**_: around **20%**
- _**Mapping "quality"**_: competitive (within 5%) with industry leaders

---

|Part 3: **Speed up** and **improvements** |
| --- |
| _Data cleanup_ |
| _**Cheap refresh**_ |
| _Machine tuning_ |
| _**Potential improvement**_ |

---

![right](images/spray.jpg)

# Data cleanup

^ There are several steps required as part of data cleaning for a graph computation like this one.

---

## **Invalid** identifiers

### 　

---

## _**Invalid** identifiers_

### Like **na** or **0** or **xyz** 
#### (or fraudulent calls to a mapping server)

^ You can analyze your graph data before doing anything and remove the most glaring invalid identifiers, but as your graph grows you'll find more and more edge cases to clean. Luckily, cleaning a graph is easy: you just destroy a component

---

## Node **pruning** 

### 　

---

## _Node **pruning**_

### To prevent huge components
#### In the cookie case, by expiring cookies not seen in `N` days

^ Any node you haven't seen in `M` days is basically useless in advertising (for some value of `M`) and we leverage this here to prevent having large components

---


## Component **destruction** 

### 　

^ 🤘

---

## _Component **destruction**_

### To limit component size artificially
#### If the data is fully clean we can assume no user has more than `M` identifiers

^ Welcome to the connected components, we've got fun and games. Destroying a component is the last resort, and only to be done for _very_ large components, and sparingly

---

![](images/dof-graph-3.webp)

### What is the **fastest way** to build a 2 billion nodes graph **daily**?　
#### 　
### 🤔

---

![](images/dof-graph-1.webp)

### _What is the **fastest** way to build a 　　　2 billion nodes graph **daily**?_

# **Not doing it**

^ 🥁

---

![right](images/chair.jpg)

# The _easy_ way

---

![fit](images/Update-1.webp)

^ We have an existing graph. We can assume it exists in some form. We have the chain data from a batch, maybe daily, maybe a few weeks or hours depending on your problem. We run the connected components algorithm on it

---

![fit](images/Update-2.webp)

^ And now we have two sets of stars, the existing ones and the new ones. But not all of them are alike

---

![fit](images/Update-3.webp)

^ Some have nodes in common between existing and new, some do not

---

![fit](images/Update-4.webp)

---

![fit](images/Update-5.webp)

^ We process them separately: those that have no nodes in common are clean, the others are tainted

---

![fit](images/Update-6.webp)

^ The clean ones are good to go, but for the tainted ones, we repeat the process of running large star - small star, with these new edges

---

![fit](images/Update-7.webp)

---

![fit](images/Update-8.webp)

^ And we end up with a (very large) consolidated graph

---

![left](images/tuning.jpg)

# Machine **tuning** 
# for _large_ graphs

^ In Apache Spark of course

---

[.build-lists: true]

![left](images/tuning.jpg)

# **Go large** _and_ **tune up**
- the process is _memory hungry_
- the process is _shuffle hungry_


---

[.build-lists: false]

![left](images/tuning.jpg)

# **Go large** _and_ **tune up**
- the process is _memory hungry_
- the process is _shuffle hungry_

## better to have few, **large**, machines

^ This is a rule of thumb: start with large machines, see how it behaves and what kind of query plans appear and then tune from there

---

[.build-lists: false]

![left](images/tuning.jpg)

# **Go large** _and_ **tune up**
- the process is _memory hungry_
- the process is _shuffle hungry_

## better to have few, **large**, machines
## and give executors **more memory** than you'd think

---

![right](images/bulb.jpg)

## Impact of **A**daptive **Q**uery **E**xecution (AQE)

### **AQE** uses runtime statistics to help the **C**ost **B**ased **O**ptimizer (CBO) and **speed up Spark**
### 　

^ The CBO tries to re-arrange queries depending on cost statistics, but needs to have updated information on all the tables. AQE keeps these up to date as the computations flow, feeding the CBO with fresh data

---

![right](images/bulb.jpg)

## Impact of **A**daptive **Q**uery **E**xecution (AQE)

### _**AQE** uses runtime statistics to help the **C**ost **B**ased **O**ptimizer (CBO) and **speed up Spark**_
### Using Spark 3.x with AQE active has a **30-40% speed up**

^ This just requires you set a flag in your SparkConf (`spark.sql.adaptive.enabled=true`)

---

[.build-lists: true]

# Further improvements
- **Easy**: Move storage to Delta Lake
- **Hard**: implement _**union-find-shuffle**_ instead of _**large star - small star**_

^ With Delta Lake we'd have the additional advantage of Z-ordering when executing certain joins, but should be a small win. UFS is supposed to be significantly faster than large star - small star, but implementing something like this requires a good reason. And here comes the end!s

---

![](images/Gene.jpg)

# __Thanks!__

---

![right fit](images/QR.png)

Get the slides from my github:

`github.com/rberenguel/`

The repository is

`identity-graphs`

---

[.hide-footer]

![fit](images/dais-feedback.jpg)

---

|**References**|
| ---: |
| [Connected Components in MapReduce and Beyond (ACM)](https://dl.acm.org/doi/10.1145/2670979.2670997) |
| [Connected Components in MapReduce and Beyond (slides)](http://mmds-data.org/presentations/2014/vassilvitskii_mmds14.pdf) |
| [Partition Aware Connected Component Computation in Distributed Systems](https://ieeexplore.ieee.org/document/7837866) |
| [Building Graphs at a Large Scale: Union Find Shuffle](https://arxiv.org/abs/2012.05430) |
| [Adaptive Query Execution: Speeding up SparkSQL at runtime](https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html) |
| [Pregel: A System for Large-Scale Graph Processing](http://www.dcs.bbk.ac.uk/~dell/teaching/cc/paper/sigmod10/p135-malewicz.pdf) |
| [GraphX](https://spark.apache.org/graphx/) |
| [GraphFrames](http://graphframes.github.io/graphframes/docs/_site/) |
| [Apache Giraph](https://giraph.apache.org) |
| [Neo4J](https://neo4j.com) |
| [AWS Neptune](https://aws.amazon.com/neptune/) |
| [Databricks' Delta Lake: high on ACID](https://mostlymaths.net/2020/10/delta-lake.html/) |

---

| **Related talks** |
| ---: |
| [Massive-Scale Entity Resolution Using the Power of Apache Spark and Graph](https://databricks.com/session/massive-scale-entity-resolution-using-the-power-of-apache-spark-and-graph) |
| [Maps and Meaning: Graph-based Entity Resolution in Apache Spark & GraphX](https://databricks.com/session_eu19/maps-and-meaning-graph-based-entity-resolution-in-apache-spark-graphx) |
| [Building Identity Graph at Scale for Programmatic Media Buying Using Apache Spark and Delta Lake](https://databricks.com/session_eu20/building-identity-graph-at-scale-for-programmatic-media-buying-using-apache-spark-and-delta-lake) |
| [Building Identity Graphs over Heterogeneous Data](https://databricks.com/session_na20/building-identity-graphs-over-heterogeneous-data) |
| [Optimize the Large Scale Graph Applications by using Apache Spark with 4-5x Performance Improvements](https://databricks.com/session_na20/optimize-the-large-scale-graph-applications-by-using-apache-spark-with-4-5x-performance-improvements) |
| [GraphFrames: Graph Queries In Spark SQL](https://databricks.com/session/graphframes-graph-queries-in-spark-sql) |
| [Using GraphX/Pregel on Browsing History to Discover Purchase Intent](https://databricks.com/session/using-graphx-pregel-on-browsing-history-to-discover-purchase-intent) |

---

| Reference | Image attribution |
| ---: | :---: |
| **Graphs** | [Ruben Berenguel](https://www.github.com/rberenguel/sketches)  😎 _(Generative art with **p5js**)_ |
| **Bulb** | [Alessandro Bianchi](https://unsplash.com/@ale_s_bianchi) _(Unsplash)_  |
| **Bubbles** | [Marko Blažević](https://unsplash.com/@kerber)  _(Unsplash)_  |
| **Chair** | [Volodymyr Tokar](https://unsplash.com/@astrovol) _(Unsplash)_ |
| **Cookie** | [Dex Ezekiel](https://unsplash.com/@dexezekiel) _(Unsplash)_ |
| **Loupe** | [Agence Olloweb](https://unsplash.com/@olloweb) _(Unsplash)_  |
| **Map** | [Timo Wielink](https://unsplash.com/@timowielink) _(Unsplash)_ |
| **Mask** | [Adnan Khan](https://unsplash.com/@adnan10) _(Unsplash)_ |
| **Newspaper** | [Rishabh Sharma](https://unsplash.com/@rishabhben) _(Unsplash)_ |
| **Party** | [Adi Goldstein](https://unsplash.com/@adigold1) _(Unsplash)_|
| **Socket** | [Kelly Sikkema](https://unsplash.com/@kellysikkema) _(Unsplash)_ |
| **Spray** | [JESHOOTS.COM](https://unsplash.com/@jeshoots) _(Unsplash)_ |
| **Tuning**| [gustavo Campos](https://unsplash.com/@gustavocpo) _(Unsplash)_ |
| **Web** | [Shannon Potter](https://unsplash.com/@cifilter) _(Unsplash)_ |

---

| **Resources** |
| --- |
| [Unicode table](https://unicode-table.com) |

---

`EOF`