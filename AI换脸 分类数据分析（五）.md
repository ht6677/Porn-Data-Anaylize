```python
'''
特殊分类视频信息分析 AI换脸
http://www.h4ck.org.cn
by obaby
obaby@mars
email:root@obaby.org.cn
date: 2020.09.11
'''

from pyspark.sql.functions import col
import altair as alt

import pandas as pd
from matplotlib import pyplot as plt
%matplotlib inline
```


```python
csv = spark.read.option("header",True).csv("hdfs://localhost:9000/data2/porn_data_movie.csv")
csv.printSchema()
```

    root
     |-- id: string (nullable = true)
     |-- create: string (nullable = true)
     |-- update: string (nullable = true)
     |-- name: string (nullable = true)
     |-- describe: string (nullable = true)
     |-- source_id: string (nullable = true)
     |-- publish_time: string (nullable = true)
     |-- play_count: string (nullable = true)
     |-- good_count: string (nullable = true)
     |-- bad_count: string (nullable = true)
     |-- link_count: string (nullable = true)
     |-- comment_count: string (nullable = true)
     |-- designation: string (nullable = true)
     |-- category_id: string (nullable = true)
     |-- porn_site_id: string (nullable = true)
     |-- uploader_id: string (nullable = true)
     |-- producer: string (nullable = true)
    



```python
csv.select('name', 'describe', 'uploader_id').show()
```

    +------------------------+------------------------+-----------+
    |                    name|                describe|uploader_id|
    +------------------------+------------------------+-----------+
    |    美女学生考试时被中出|                    null|          1|
    |      无套中出内射（上）|            无套中出内射|          2|
    |      无套中出内射（下）|            无套中出内射|          2|
    |                极品嫩妹|                    null|          3|
    |                骚妹妹 7|                    null|          3|
    |漂亮美女完美身材甜美声音|漂亮美女完美身材甜美声音|          4|
    |          jk捆绑萝莉束缚|                    null|          1|
    |            最纯萝莉性爱|                    null|          1|
    |          整容脸制服美女|                    null|          1|
    |             星 调教萝莉|                    null|          5|
    |        大一学生寝室自慰|                    null|          6|
    |    超性感美女给你打飞机|                    null|          6|
    |      超可爱萝莉下海资源|                    null|          6|
    |      迷倒女儿然后慢慢操|                    null|          6|
    |   精灵做爱高潮–影视剪辑|                    null|          7|
    |        小萝莉被插到大叫|                    null|          6|
    |    睡醒和白嫩女友来一发|                    null|          6|
    |  双马尾萝莉甜美自慰诱惑|                    null|          8|
    |        调教双马尾小萝莉|                    null|          1|
    |    黑丝小萝莉最喜欢玩具|                    null|          9|
    +------------------------+------------------------+-----------+
    only showing top 20 rows
    



```python
# 分类信息读取 
category_csv = spark.read.option("header",True).csv("hdfs://localhost:9000/data2/porn_data_category.csv")
```


```python
category_csv.printSchema()
```

    root
     |-- id: string (nullable = true)
     |-- create: string (nullable = true)
     |-- update: string (nullable = true)
     |-- name: string (nullable = true)
     |-- key: string (nullable = true)
     |-- describe: string (nullable = true)
    



```python
movie_csv = csv.withColumnRenamed('name','movie_name')
movie_csv.select('movie_name', 'category_id', 'id').show()
```

    +---+--------------------+--------------------+------------------------+------------------------+---------+------------+----------+----------+---------+----------+-------------+-----------+-----------+------------+-----------+--------+
    | id|              create|              update|              movie_name|                describe|source_id|publish_time|play_count|good_count|bad_count|link_count|comment_count|designation|category_id|porn_site_id|uploader_id|producer|
    +---+--------------------+--------------------+------------------------+------------------------+---------+------------+----------+----------+---------+----------+-------------+-----------+-----------+------------+-----------+--------+
    |  1|7/5/2020 05:20:14...|7/5/2020 05:20:14...|    美女学生考试时被中出|                    null|    76009|  1588819417|      2566|        14|        2|        48|            0|       null|          1|           1|          1|    null|
    |  2|7/5/2020 05:21:04...|7/5/2020 05:21:04...|      无套中出内射（上）|            无套中出内射|    76021|  1588757668|     11199|        58|        3|       338|            0|       null|          1|           1|          2|    null|
    |  3|7/5/2020 05:21:06...|7/5/2020 05:21:06...|      无套中出内射（下）|            无套中出内射|    76028|  1588757665|      7067|        39|        3|       310|            0|       null|          1|           1|          2|    null|
    |  4|7/5/2020 05:21:12...|7/5/2020 05:21:13...|                极品嫩妹|                    null|    76053|  1588756935|      3372|        18|        2|       159|            0|       null|          1|           1|          3|    null|
    |  5|7/5/2020 05:21:18...|7/5/2020 05:21:19...|                骚妹妹 7|                    null|    76051|  1588756876|      1975|        10|        0|        53|            0|       null|          1|           1|          3|    null|
    |  6|7/5/2020 05:22:13...|7/5/2020 05:22:13...|漂亮美女完美身材甜美声音|漂亮美女完美身材甜美声音|    75971|  1588744135|     17097|        31|        3|       222|            0|       null|          1|           1|          4|    null|
    |  7|7/5/2020 05:22:19...|7/5/2020 05:22:19...|          jk捆绑萝莉束缚|                    null|    75342|  1588739354|      8362|        29|        6|       201|            0|       null|          1|           1|          1|    null|
    |  8|7/5/2020 05:22:23...|7/5/2020 05:22:23...|            最纯萝莉性爱|                    null|    75112|  1588739158|     10329|        61|        5|       434|            0|       null|          1|           1|          1|    null|
    |  9|7/5/2020 05:22:30...|7/5/2020 05:22:31...|          整容脸制服美女|                    null|    75104|  1588739080|      5297|        26|        2|       221|            0|       null|          1|           1|          1|    null|
    | 10|7/5/2020 05:22:38...|7/5/2020 05:22:38...|             星 调教萝莉|                    null|    75563|  1588738220|      4725|        31|        5|       166|            0|       null|          1|           1|          5|    null|
    | 11|7/5/2020 05:22:44...|7/5/2020 05:22:45...|        大一学生寝室自慰|                    null|    75601|  1588686744|     20318|       152|        4|       773|            0|       null|          1|           1|          6|    null|
    | 12|7/5/2020 05:22:47...|7/5/2020 05:22:48...|    超性感美女给你打飞机|                    null|    75603|  1588686520|     17302|        54|        7|       333|            0|       null|          1|           1|          6|    null|
    | 13|7/5/2020 05:22:53...|7/5/2020 05:22:53...|      超可爱萝莉下海资源|                    null|    75747|  1588686493|     18133|        98|       10|       497|            0|       null|          1|           1|          6|    null|
    | 14|7/5/2020 05:22:56...|7/5/2020 05:22:57...|      迷倒女儿然后慢慢操|                    null|    75611|  1588686476|     59674|       182|       25|       793|            0|       null|          1|           1|          6|    null|
    | 15|7/5/2020 05:22:59...|7/5/2020 05:23:00...|   精灵做爱高潮–影视剪辑|                    null|    75543|  1588686387|      8767|       100|        7|       152|            0|       null|          1|           1|          7|    null|
    | 16|7/5/2020 05:23:16...|7/5/2020 05:23:16...|        小萝莉被插到大叫|                    null|    75307|  1588670775|     14213|        71|        8|       360|            0|       null|          1|           1|          6|    null|
    | 17|7/5/2020 05:23:21...|7/5/2020 05:23:22...|    睡醒和白嫩女友来一发|                    null|    75318|  1588668616|      5350|        30|        3|       185|            0|       null|          1|           1|          6|    null|
    | 18|7/5/2020 05:23:26...|7/5/2020 05:23:27...|  双马尾萝莉甜美自慰诱惑|                    null|    75382|  1588652017|     25311|       190|       10|       890|            0|       null|          1|           1|          8|    null|
    | 19|7/5/2020 05:23:28...|7/5/2020 05:23:29...|        调教双马尾小萝莉|                    null|    74932|  1588644358|     29114|       180|       19|       783|            0|       null|          1|           1|          1|    null|
    | 20|7/5/2020 05:23:35...|7/5/2020 05:23:36...|    黑丝小萝莉最喜欢玩具|                    null|    74812|  1588585581|     17084|       141|       10|       883|            0|       null|          1|           1|          9|    null|
    +---+--------------------+--------------------+------------------------+------------------------+---------+------------+----------+----------+---------+----------+-------------+-----------+-----------+------------+-----------+--------+
    only showing top 20 rows
    



```python
movie_cat_rdd = movie_csv.select('movie_name','category_id').join(category_csv, movie_csv.category_id == category_csv.id, "inner")


```


```python
movie_cat_rdd.select('movie_name', 'name', 'id').show()
```

    +------------------------+------+---+
    |              movie_name|  name| id|
    +------------------------+------+---+
    |    美女学生考试时被中出|萝莉系|  1|
    |      无套中出内射（上）|萝莉系|  1|
    |      无套中出内射（下）|萝莉系|  1|
    |                极品嫩妹|萝莉系|  1|
    |                骚妹妹 7|萝莉系|  1|
    |漂亮美女完美身材甜美声音|萝莉系|  1|
    |          jk捆绑萝莉束缚|萝莉系|  1|
    |            最纯萝莉性爱|萝莉系|  1|
    |          整容脸制服美女|萝莉系|  1|
    |             星 调教萝莉|萝莉系|  1|
    |        大一学生寝室自慰|萝莉系|  1|
    |    超性感美女给你打飞机|萝莉系|  1|
    |      超可爱萝莉下海资源|萝莉系|  1|
    |      迷倒女儿然后慢慢操|萝莉系|  1|
    |   精灵做爱高潮–影视剪辑|萝莉系|  1|
    |        小萝莉被插到大叫|萝莉系|  1|
    |    睡醒和白嫩女友来一发|萝莉系|  1|
    |  双马尾萝莉甜美自慰诱惑|萝莉系|  1|
    |        调教双马尾小萝莉|萝莉系|  1|
    |    黑丝小萝莉最喜欢玩具|萝莉系|  1|
    +------------------------+------+---+
    only showing top 20 rows
    



```python
import jieba.posseg as psg

def get_person_name_from_line(line):
    words = psg.cut(str(line))
    l =[]
    for x in words:
        if x.flag =='nr':
            l.append(x.word)
        # print(x.word, x.flag)
    return l
    
```


```python
# 测试文字 http://m.lewenb.com/wapbook-1315-235763/
# document.body.innerText
w2 = get_person_name_from_line('朱宜锐的一只大手，隔着一层绵薄滑软的抚握住范冰冰那一只弹挺柔软的，他的手轻而不急地揉捏着……手掌间传来一阵坚挺结实、柔软无比而又充满弹性的美妙肉感，令他血脉贲张。看见澜那线条优美的秀丽桃腮上，一抹醉人的晕红正逐渐蔓衍到她那美艳动人的绝色娇靥上，他不由得色心一荡，他的手指逐渐收拢，轻轻地用两根手指轻抚范冰冰下那傲挺的峰顶，打着圈的轻抚揉压，找到那一粒娇小玲珑的挺突之巅的。他两根手指轻轻地夹住范冰冰那娇软柔小的，温柔而有技巧地一阵揉搓、轻捏。范冰冰被那从敏感地带的玉上传来的异样的感觉弄得浑身如被虫噬。一想到就连自己平常一个人都不好意思久看，不敢轻触的娇小被这样一个陌生而又恶心的男人（至少在她看来是这样）肆意揉搓轻抚，芳心不觉又感到万分的羞涩和莫名的刺激。朱宜锐一面揉捏着范冰冰那娇小的，一面继续在她的深处抽动着……')
print(w2)
```

    ['朱宜锐', '范冰冰', '贲', '张', '范冰冰', '范冰冰', '范冰冰', '朱宜锐', '范冰冰']



```python
category_csv.filter(col('name')== 'AI换脸').show()
```

    +---+--------------------+--------------------+------+----------+--------+
    | id|              create|              update|  name|       key|describe|
    +---+--------------------+--------------------+------+----------+--------+
    |  9|7/5/2020 06:51:12...|27/8/2020 03:32:4...|AI换脸|aihuanlian|    null|
    +---+--------------------+--------------------+------+----------+--------+
    



```python
movie_cat_rdd.show()
```

    +------------------------+-----------+---+--------------------+--------------------+------+-------+--------+
    |              movie_name|category_id| id|              create|              update|  name|    key|describe|
    +------------------------+-----------+---+--------------------+--------------------+------+-------+--------+
    |    美女学生考试时被中出|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |      无套中出内射（上）|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |      无套中出内射（下）|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |                极品嫩妹|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |                骚妹妹 7|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |漂亮美女完美身材甜美声音|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |          jk捆绑萝莉束缚|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |            最纯萝莉性爱|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |          整容脸制服美女|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |             星 调教萝莉|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |        大一学生寝室自慰|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |    超性感美女给你打飞机|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |      超可爱萝莉下海资源|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |      迷倒女儿然后慢慢操|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |   精灵做爱高潮–影视剪辑|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |        小萝莉被插到大叫|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |    睡醒和白嫩女友来一发|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |  双马尾萝莉甜美自慰诱惑|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |        调教双马尾小萝莉|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    |    黑丝小萝莉最喜欢玩具|          1|  1|7/5/2020 05:20:14...|27/8/2020 03:31:2...|萝莉系|luolixi|    null|
    +------------------------+-----------+---+--------------------+--------------------+------+-------+--------+
    only showing top 20 rows
    



```python

```


```python
# 只分析AI换脸分类下的数据，虽然会有一部分遗漏，如果全部数据都进行解析会引入大量的无关数据（主要由于分词精度不够）
name_rdd = movie_cat_rdd.filter(col('category_id')=='9').select('movie_name').rdd
```


```python
name_word_rdd = name_rdd.flatMap(get_person_name_from_line)
```


```python
name_word_rdd.take(20)
```




    ['林允儿',
     '杨幂',
     '祖儿',
     '刘亦菲',
     '明星',
     '陈乔恩',
     '容祖儿',
     '林允儿',
     '明星',
     '宋',
     '王鸥',
     '迪丽',
     '刘亦菲',
     '明星',
     '迪丽',
     '陈乔恩',
     '刘亦菲',
     '刘亦菲',
     '迪丽',
     '陈乔恩']




```python
# map reduce 进行词频统计
name_counts_rdd = name_word_rdd.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)

```


```python
print('姓名统计一共:', str(name_counts_rdd.count()))
```

    姓名统计一共: 100



```python
name_counts_rdd.sortBy(lambda a: a[1],ascending=False).collect()[:20]
```




    [('明星', 33),
     ('杨幂', 27),
     ('迪丽', 27),
     ('刘亦菲', 17),
     ('高清', 14),
     ('杨颖', 13),
     ('林允儿', 10),
     ('鞠', 8),
     ('杨', 8),
     ('白虎', 6),
     ('刘诗诗', 6),
     ('宋雨琦', 6),
     ('佟丽娅', 6),
     ('宋', 5),
     ('陈乔恩', 5),
     ('爽', 5),
     ('范冰冰', 4),
     ('刘涛', 4),
     ('林志玲', 4),
     ('宋祖儿', 3)]




```python
name_counts_df = name_counts_rdd.toDF().toPandas()

```


```python
name_counts_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>_1</th>
      <th>_2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>杨幂</td>
      <td>27</td>
    </tr>
    <tr>
      <th>1</th>
      <td>祖儿</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>明星</td>
      <td>33</td>
    </tr>
    <tr>
      <th>3</th>
      <td>容祖儿</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>宋</td>
      <td>5</td>
    </tr>
  </tbody>
</table>
</div>




```python
# 视频标签数量展示
bars=alt.Chart(name_counts_df).mark_bar().encode(
    x=alt.X('_2', title='视频数量'),
    y=alt.Y('_1', title='明星姓名', sort='-x')
)
text = bars.mark_text(align='left', baseline='middle', dx=3).encode(text='_2')
(bars + text).properties(height=1400, width=800)
```





<div id="altair-viz-a05a250c1b0c4ad59f291ee91c7598cc"></div>
<script type="text/javascript">
  (function(spec, embedOpt){
    let outputDiv = document.currentScript.previousElementSibling;
    if (outputDiv.id !== "altair-viz-a05a250c1b0c4ad59f291ee91c7598cc") {
      outputDiv = document.getElementById("altair-viz-a05a250c1b0c4ad59f291ee91c7598cc");
    }
    const paths = {
      "vega": "https://cdn.jsdelivr.net/npm//vega@5?noext",
      "vega-lib": "https://cdn.jsdelivr.net/npm//vega-lib?noext",
      "vega-lite": "https://cdn.jsdelivr.net/npm//vega-lite@4.8.1?noext",
      "vega-embed": "https://cdn.jsdelivr.net/npm//vega-embed@6?noext",
    };

    function loadScript(lib) {
      return new Promise(function(resolve, reject) {
        var s = document.createElement('script');
        s.src = paths[lib];
        s.async = true;
        s.onload = () => resolve(paths[lib]);
        s.onerror = () => reject(`Error loading script: ${paths[lib]}`);
        document.getElementsByTagName("head")[0].appendChild(s);
      });
    }

    function showError(err) {
      outputDiv.innerHTML = `<div class="error" style="color:red;">${err}</div>`;
      throw err;
    }

    function displayChart(vegaEmbed) {
      vegaEmbed(outputDiv, spec, embedOpt)
        .catch(err => showError(`Javascript Error: ${err.message}<br>This usually means there's a typo in your chart specification. See the javascript console for the full traceback.`));
    }

    if(typeof define === "function" && define.amd) {
      requirejs.config({paths});
      require(["vega-embed"], displayChart, err => showError(`Error loading script: ${err.message}`));
    } else if (typeof vegaEmbed === "function") {
      displayChart(vegaEmbed);
    } else {
      loadScript("vega")
        .then(() => loadScript("vega-lite"))
        .then(() => loadScript("vega-embed"))
        .catch(showError)
        .then(() => displayChart(vegaEmbed));
    }
  })({"config": {"view": {"continuousWidth": 400, "continuousHeight": 300}}, "layer": [{"mark": "bar", "encoding": {"x": {"type": "quantitative", "field": "_2", "title": "\u89c6\u9891\u6570\u91cf"}, "y": {"type": "nominal", "field": "_1", "sort": "-x", "title": "\u660e\u661f\u59d3\u540d"}}}, {"mark": {"type": "text", "align": "left", "baseline": "middle", "dx": 3}, "encoding": {"text": {"type": "quantitative", "field": "_2"}, "x": {"type": "quantitative", "field": "_2", "title": "\u89c6\u9891\u6570\u91cf"}, "y": {"type": "nominal", "field": "_1", "sort": "-x", "title": "\u660e\u661f\u59d3\u540d"}}}], "data": {"name": "data-9c22a76496750b6f01daa248fc436e3d"}, "height": 1400, "width": 800, "$schema": "https://vega.github.io/schema/vega-lite/v4.8.1.json", "datasets": {"data-9c22a76496750b6f01daa248fc436e3d": [{"_1": "\u6768\u5e42", "_2": 27}, {"_1": "\u7956\u513f", "_2": 1}, {"_1": "\u660e\u661f", "_2": 33}, {"_1": "\u5bb9\u7956\u513f", "_2": 2}, {"_1": "\u5b8b", "_2": 5}, {"_1": "\u738b\u9e25", "_2": 2}, {"_1": "\u9648\u598d", "_2": 1}, {"_1": "\u5e0c\u6f6e", "_2": 1}, {"_1": "\u5b59\u4fea", "_2": 1}, {"_1": "\u67f3\u5ca9", "_2": 1}, {"_1": "\u6797\u73cd\u5a1c", "_2": 1}, {"_1": "\u5c0f\u5976\u59b9", "_2": 1}, {"_1": "\u6768\u9896", "_2": 13}, {"_1": "\u5b8b\u7956\u513f", "_2": 3}, {"_1": "\u6cf0\u52d2", "_2": 1}, {"_1": "\u666f\u751c", "_2": 1}, {"_1": "\u6d2a\u6c34", "_2": 1}, {"_1": "\u767d\u864e", "_2": 6}, {"_1": "\u8001\u516c", "_2": 1}, {"_1": "\u5f20\u5f00", "_2": 1}, {"_1": "\u5c0f\u599e", "_2": 1}, {"_1": "\u9ed1\u5be1\u5987", "_2": 2}, {"_1": "\u5927\u79c0", "_2": 1}, {"_1": "\u5999\u4e3d", "_2": 1}, {"_1": "\u5c0f\u59d0\u59d0", "_2": 1}, {"_1": "\u65af\u5854\u514b", "_2": 1}, {"_1": "\u53e4\u4e3d", "_2": 1}, {"_1": "\u53cc\u7a74\u9f50", "_2": 1}, {"_1": "\u7f8e\u5c11\u5973", "_2": 3}, {"_1": "\u9ad8\u6e05", "_2": 14}, {"_1": "\u53cc\u59dd", "_2": 1}, {"_1": "\u51cc\u8fb1", "_2": 1}, {"_1": "\u5b8b\u6167\u55ac", "_2": 2}, {"_1": "\u6069", "_2": 1}, {"_1": "\u5218\u8bd7\u8bd7", "_2": 6}, {"_1": "\u674e\u5c0f\u7490", "_2": 1}, {"_1": "\u666f\u7530", "_2": 1}, {"_1": "\u674e\u5609\u6b23", "_2": 1}, {"_1": "\u68ee", "_2": 1}, {"_1": "\u5468\u5b50\u745c", "_2": 2}, {"_1": "\u5289\u4ea6\u83f2", "_2": 1}, {"_1": "\u4f0a\u4e3d\u838e\u767d", "_2": 1}, {"_1": "\u5b8b\u96e8\u7426", "_2": 6}, {"_1": "\u595a\u68a6\u7476", "_2": 1}, {"_1": "\u661f\u5973\u90ce", "_2": 1}, {"_1": "\u598d", "_2": 1}, {"_1": "\u6731\u8335", "_2": 1}, {"_1": "\u795d\u7eea\u4e39", "_2": 2}, {"_1": "\u90fd\u7075", "_2": 3}, {"_1": "\u6768\u9896\u53cc", "_2": 1}, {"_1": "\u9648\u94b0\u742a", "_2": 1}, {"_1": "\u53cc\u98de", "_2": 1}, {"_1": "\u767d\u4e1d", "_2": 1}, {"_1": "\u6e29\u6cc9", "_2": 1}, {"_1": "\u5e08\u751f", "_2": 1}, {"_1": "\u6797\u5141\u513f", "_2": 10}, {"_1": "\u5218\u4ea6\u83f2", "_2": 17}, {"_1": "\u9648\u4e54\u6069", "_2": 5}, {"_1": "\u8fea\u4e3d", "_2": 27}, {"_1": "\u97a0", "_2": 8}, {"_1": "\u7a0b\u6f47", "_2": 3}, {"_1": "\u6c5f", "_2": 2}, {"_1": "\u4f5f\u4e3d\u5a05", "_2": 6}, {"_1": "\u8d75\u4e3d\u9896", "_2": 2}, {"_1": "\u8303\u51b0\u51b0", "_2": 4}, {"_1": "\u9b4f\u5927\u52cb", "_2": 1}, {"_1": "\u97a0\u5a67\u709c", "_2": 1}, {"_1": "\u5218\u6d9b", "_2": 4}, {"_1": "\u6768", "_2": 8}, {"_1": "\u723d", "_2": 5}, {"_1": "\u9c8d\u9c8d", "_2": 2}, {"_1": "\u9093\u7d2b\u68cb", "_2": 3}, {"_1": "\u9ec4\u6653", "_2": 2}, {"_1": "\u4e4b\u604b", "_2": 2}, {"_1": "\u6797\u5fd7\u73b2", "_2": 4}, {"_1": "\u674e", "_2": 2}, {"_1": "\u4f5f\u4e9a\u4e3d", "_2": 1}, {"_1": "\u9ad8\u6f6e", "_2": 3}, {"_1": "\u7389\u8db3", "_2": 1}, {"_1": "\u97e9\u96ea", "_2": 1}, {"_1": "\u592a\u598d", "_2": 2}, {"_1": "\u827e\u739b\u534e", "_2": 1}, {"_1": "\u745c", "_2": 1}, {"_1": "\u9b91\u9b91", "_2": 1}, {"_1": "\u6768\u7d2b", "_2": 1}, {"_1": "\u5141\u513f", "_2": 1}, {"_1": "\u674e\u4e00\u6850", "_2": 2}, {"_1": "\u8303\u82e5\u82e5", "_2": 1}, {"_1": "\u9ad8\u5706\u5706", "_2": 2}, {"_1": "\u5f20\u96e8", "_2": 2}, {"_1": "\u9b91", "_2": 1}, {"_1": "\u52a0\u6735", "_2": 1}, {"_1": "\u9648", "_2": 3}, {"_1": "\u75f4\u6c49", "_2": 1}, {"_1": "\u9648\u9759", "_2": 1}, {"_1": "\u6c5f\u7409\u5f71", "_2": 1}, {"_1": "\u97e9\u56e2", "_2": 1}, {"_1": "\u674e\u627f\u5229", "_2": 1}, {"_1": "\u989c\u5c04", "_2": 1}, {"_1": "\u5b59\u827a\u73cd", "_2": 1}]}}, {"mode": "vega-lite"});
</script>




```python
# 迪丽-> 迪丽热巴
# 对于分词之后的姓名可以做进一步的解析，通过人工分析可以修正解析到的错误姓名。
# https://intellipaat.com/community/11489/filter-spark-dataframe-on-string-contains
movie_cat_rdd.filter(col('movie_name').contains('迪丽')).select('movie_name').show()
```

    +-----------------------------------+
    |                         movie_name|
    +-----------------------------------+
    |重金购买长相酷似女星迪丽XX的性感...|
    |           迪丽热巴刘亦菲等明星合集|
    |             迪丽热巴换上盛装自己嗨|
    |               Al迪丽热巴大长腿中出|
    |                       低配迪丽热巴|
    |                    AI换脸 迪丽热巴|
    |                           迪丽热巴|
    |            AI换脸 迪丽热巴大战炮机|
    |                 AI迪丽热巴自慰绝美|
    |     [AI换脸] 迪丽热巴！演艺圈淫...|
    |                 [AI换脸] 迪丽热巴2|
    |【明星淫梦】人工智能AI让女神们下...|
    |                 【AI换脸】迪丽热巴|
    |            国产，酷似迪丽热巴 爆菊|
    |                    AI换脸 迪丽热巴|
    |                 [AI换脸] 迪丽热巴3|
    |           迪丽热巴唯美身材激情自慰|
    |                           迪丽热巴|
    |                           迪丽热巴|
    |                  迪丽热巴 智能换脸|
    +-----------------------------------+
    only showing top 20 rows
    



```python
movie_cat_rdd.filter(col('movie_name').contains('鞠')).select('movie_name').show()
```

    +-------------------------------+
    |                     movie_name|
    +-------------------------------+
    |             宋轶鞠婧祎模特洗礼|
    |               鞠婧祎和一群男人|
    |【9uu神手】AI换脸之鞠婧祎 骚...|
    |                  AI换脸 鞠婧炜|
    |          【AI】鞠婧祎 短发空乘|
    |         Al换脸鞠婧祎双胞胎服侍|
    |                         鞠婧祎|
    |                  AI换脸鞠婧祎 |
    |          原创ai-宋轶鞠婧祎长腿|
    |         ai换脸鞠婧祎干朋友妻子|
    |           AI换脸女神鞠婧祎合集|
    |           AI原创鞠婧祎激情颜射|
    +-------------------------------+
    



```python
# 除了部分人名解析有问题，另外还有一部分数据原本不是人名，解析成了人名。例如玉足、痴汉等等~~
# 通过视频数量侧面也反映出了女明星在宅男中的热度？或者流行程度？
```
