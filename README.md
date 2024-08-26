![Introduction-to-ETL-pipelines-1440x425-1](https://github.com/user-attachments/assets/f8fbcddb-8e79-4aae-8192-0acafbc0404e)


# reddit_sentiments - ETL Pipeline

**reddit_sentiments** is a project that analyzes sentiment trends across various Reddit subreddits in real-time. By utilizing a combination of Apache Kafka for messaging, Apache Spark for stream processing, and Cassandra for data storage, this pipeline delivers insights into how sentiment evolves over time on different topics.

### **Technology Stack**
- Python: 3.x (for PRAW, NLTK, etc.)
- Apache Kafka: 2.x
- Apache Spark: 3.x
- Cassandra: 3.x
- Grafana: 8.x

## 🚀 **Project Flow Overview**

### 🛠️ **Data Ingestion**
A Python application called `reddit_producer` connects to the Reddit API using credentials. It retrieves comments from specified Reddit topics, converts them into JSON format, and sends the serialized JSON messages to a Kafka broker. The **PRAW** Python library is used for interacting with the Reddit API. **Multi-threading** is implemented to stream comments from multiple subreddits simultaneously.

### 📡 **Message Broker**
The **Kafka broker** receives messages from the `reddit_producer` and writes them into topic `redditcomments`. **Zookeeper** is launched before Kafka to manage Kafka metadata.

### ⚙️ **Stream Processor**
A **Spark** deployment is initiated to consume and process the data from the Kafka topic `redditcomments`. The PySpark script `spark/stream_processor.py` handles data consumption and processing, performing sentiment analysis on the comments using the **NLTK** library. The processed data is then written to **Cassandra** tables.

### 🗄️ **Processed Data Storage**
A **Cassandra** cluster is used to store and serve the processed data from the Spark job in `reddit` keyspace. The raw comments data is written to the `reddit.comments` table, and the aggregated sentiment data (e.g., average sentiment score for each subreddit) is written to the `reddit.subreddit_sentiment_avg` table.

### 📊 **Data Visualization**
**Grafana** connects to the Cassandra database. It queries the aggregated data from Cassandra in real time and presents it visually to users through a dashboard. This allows for the live analysis of sentiment trends across different subreddits.


### Architecture Diagram
![reddit drawio (2)](https://github.com/user-attachments/assets/497177d4-46ae-4486-bb18-f38cfdab2faf)


## Live Dashboard Outputs

<table>
  <tr>
    <td align="center">
      <img src="https://github.com/user-attachments/assets/30ab0a1d-81f4-4a4a-8030-734acad51976" width="500"/><br>
      <b>Snap after 5 min</b>
    </td>
    <td align="center">
      <img src="https://github.com/user-attachments/assets/76e78039-2d7c-4bba-b77a-d7462ea3172b" width="500"/><br>
      <b>Snap after 15 min</b>
    </td>
  </tr>
  <tr>
    <td colspan="2" align="center">
      <img src="https://github.com/user-attachments/assets/830c4d0f-8771-4e09-8ac2-ddff8b758249" width="500"/><br>
      <b>Snap after 30 min</b>
    </td>
  </tr>
</table>

## 🌟 Future Enhancements

### 🚨 **Real-time Alerts and Notifications:**
- **Anomaly Detection:**  
  Implement real-time anomaly detection to trigger alerts when unusual sentiment trends are detected in specific subreddits, enabling quicker response to emerging issues.
  
- **Sentiment-Based Notifications:**  
  Develop a notification system that sends alerts or emails when certain sentiment thresholds are crossed (e.g., a sudden spike in negative sentiment).

### 🔍 **Advanced Sentiment Analysis Techniques:**
- **Deep Learning Models:**  
  Integrate more advanced sentiment analysis models such as BERT or GPT-based models to improve the accuracy of sentiment classification.
  
- **Custom Sentiment Scoring:**  
  Develop a custom sentiment scoring mechanism that takes into account context, sarcasm, and complex language patterns, improving the depth of analysis.

### 📊 **Topic Modeling and Trend Analysis:**
- **Topic Detection:**  
  Implement topic modeling algorithms like LDA (Latent Dirichlet Allocation) to automatically discover prevalent topics within subreddits and analyze sentiment trends around these topics.

### 🌐 **Additional Data Sources:**
- **Multi-platform Integration:**  
  Expand the pipeline to ingest and analyze data from other social media platforms such as Twitter, Facebook, or YouTube, allowing for a more comprehensive sentiment analysis across the web.

### 📈 **Enhanced Visualization:**
- **Interactive Dashboards:**  
  Create more interactive and customizable dashboards in Grafana, allowing users to filter data by time range, subreddit, or specific keywords.
  
- **Geographical Visualization:**  
  Incorporate geospatial data to visualize sentiment trends on a map, showing how opinions vary across different regions or countries.









