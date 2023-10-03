<center>
  <h1>
    <a href="https://github.com/myshasolin/streaming_data_processing">
      здесь упражнения и самостоятельные проекты, выполненные в рамках курса "Потоковая обработка данных", от простого (1) к сложному (8):
    </a>
  </h1>
</center>

<table style="border: 2px double;">
  <tr>
    <th></th>
    <th>тема</th>
    <th>описание</th>
    <th>инструменты</th>
  </tr>
  <tr>
    <td>
      1
    </td>
    <td>
      <a href="https://github.com/myshasolin/streaming_data_processing/tree/main/1%20Spark%20Streaming.%20%D0%A2%D0%B5%D1%81%D1%82%D0%BE%D0%B2%D1%8B%D0%B5%20%D1%81%D1%82%D1%80%D0%B8%D0%BC%D1%8B%2C%20%D1%87%D1%82%D0%B5%D0%BD%D0%B8%D0%B5%20%D1%84%D0%B0%D0%B9%D0%BB%D0%BE%D0%B2%20%D0%B2%20%D1%80%D0%B5%D0%B0%D0%BB%D1%8C%D0%BD%D0%BE%D0%BC%20%D0%B2%D1%80%D0%B5%D0%BC%D0%B5%D0%BD%D0%B8">
        Spark Streaming. Тестовые стримы, чтение файлов в реальном времени
      </a>
    </td>
    <td>
      два файла:<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/1%20Spark%20Streaming.%20%D0%A2%D0%B5%D1%81%D1%82%D0%BE%D0%B2%D1%8B%D0%B5%20%D1%81%D1%82%D1%80%D0%B8%D0%BC%D1%8B%2C%20%D1%87%D1%82%D0%B5%D0%BD%D0%B8%D0%B5%20%D1%84%D0%B0%D0%B9%D0%BB%D0%BE%D0%B2%20%D0%B2%20%D1%80%D0%B5%D0%B0%D0%BB%D1%8C%D0%BD%D0%BE%D0%BC%20%D0%B2%D1%80%D0%B5%D0%BC%D0%B5%D0%BD%D0%B8/console">console</a> — в терминале создаю Spark Structured Streaming-приложение на батчи в 5 строк в секунду с добавлением к ним нового столбца, источник вывода - окно терминала<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/1%20Spark%20Streaming.%20%D0%A2%D0%B5%D1%81%D1%82%D0%BE%D0%B2%D1%8B%D0%B5%20%D1%81%D1%82%D1%80%D0%B8%D0%BC%D1%8B%2C%20%D1%87%D1%82%D0%B5%D0%BD%D0%B8%D0%B5%20%D1%84%D0%B0%D0%B9%D0%BB%D0%BE%D0%B2%20%D0%B2%20%D1%80%D0%B5%D0%B0%D0%BB%D1%8C%D0%BD%D0%BE%D0%BC%20%D0%B2%D1%80%D0%B5%D0%BC%D0%B5%D0%BD%D0%B8/console_stream_file">console_stream_file</a> — читаю в стриме csv-файл, который представляет из себя 10 первых строчек датасета load_iris с добавлением к ним строкового столбца с буквами (просто чтоб в схеме можно было указать разные типы данных, а не только DoubleType)
    </td>
    <td>
      PySpark, Spark Structured Streaming, HDFS, Docker, bash
    </td>
  </tr>
  <tr>
    <td>
      2
    </td>
    <td>
      <a href="https://github.com/myshasolin/streaming_data_processing/tree/main/2%20Kafka.%20%D0%90%D1%80%D1%85%D0%B8%D1%82%D0%B5%D0%BA%D1%82%D1%83%D1%80%D0%B0">
        Kafka. Архитектура
      </a>
    </td>
    <td>
      три файла:<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/2%20Kafka.%20%D0%90%D1%80%D1%85%D0%B8%D1%82%D0%B5%D0%BA%D1%82%D1%83%D1%80%D0%B0/exercise">exercise</a> — здесь пошагово расписано выполнение заданий и скопированы команды из терминала и результаты из отработки<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/2%20Kafka.%20%D0%90%D1%80%D1%85%D0%B8%D1%82%D0%B5%D0%BA%D1%82%D1%83%D1%80%D0%B0/producer-consumer.png">producer-consumer.png</a> — это просто скрин, показывающий два окна терминала — в одном produser, в другом consumer<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/2%20Kafka.%20%D0%90%D1%80%D1%85%D0%B8%D1%82%D0%B5%D0%BA%D1%82%D1%83%D1%80%D0%B0/file_to_kafka.py">file_to_kafka.py</a> — скрипт, с помощью которого я забрал файл из HDFS, преобразовал csv в JSON и записал в топик. Хоть скрипт и одноразовый, но он понадобился, чтоб использовать Pandas и прочие питонячьи хитрости
    </td>
    <td>
      Python, PySpark, Kafka, HDFS, Docker, bash
    </td>
  </tr>
  <tr>
    <td>
      3
    </td>
    <td>
      <a href="https://github.com/myshasolin/streaming_data_processing/tree/main/3%20Spark%20Streaming.%20%D0%A7%D1%82%D0%B5%D0%BD%D0%B8%D0%B5%20Kafka">
        Spark Streaming. Чтение Kafka
      </a>
    </td>
    <td>
      три файла:<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/3%20Spark%20Streaming.%20%D0%A7%D1%82%D0%B5%D0%BD%D0%B8%D0%B5%20Kafka/exercise_spotify">exercise_spotify</a> — это текстовый файл, в который я собрал все команды и результат их работы в терминале. Я там перевожу csv в json, создаю топик, читаю его то сверху, то снизу, в том числе и с чекпоинтом. В конце заглядываю в checkpoint directory на HDFS<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/3%20Spark%20Streaming.%20%D0%A7%D1%82%D0%B5%D0%BD%D0%B8%D0%B5%20Kafka/csv_to_json.py">csv_to_json.py</a> — это скрипт, с помощью которого я как раз перевожу csv в json<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/3%20Spark%20Streaming.%20%D0%A7%D1%82%D0%B5%D0%BD%D0%B8%D0%B5%20Kafka/readStream_starting_offset.png">readStream_starting_offset.png</a> — это скрин с экрана, на котором видно 2 открытых терминала, на одном из которых я запускаю стрим с конца, чтоб читать только новые сообщения батчами по 5 шт. раз в 10 секунд, а на другом заливаю в топик новые сообщение (20 штук). Скрин мелковат, но более-менее понятен, на нём видно код создания стрима из kafka и все новые появившиеся батчи
    </td>
    <td>
      Python, PySpark, Kafka, Spark Structured Streaming, HDFS, Docker, bash
    </td>
  </tr>
  <tr>
    <td>
      4
    </td>
    <td>
      <a href="https://github.com/myshasolin/streaming_data_processing/tree/main/4%20Spark%20Streaming.%20Sinks">
        Spark Streaming. Sinks
      </a>
    </td>
    <td>
      два файла:<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/4%20Spark%20Streaming.%20Sinks/exercise_notebooks">exercise_notebooks</a> — скопированные строки из двух терминалов. Перед каждым шагом оставил комментарии, что делаю, и последовательно рассматриваю всех "канонических" новых получателей<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/4%20Spark%20Streaming.%20Sinks/csv_to_json.py">csv_to_json.py</a> — это модуль по переделке csv в json. Я его немного переписал, так как, оказалось, что в прошлой его версии, все значения в JSON сохранялись как строки. Теперь же всё хорошо — int сохраняется как int, float как float, а str как str
    </td>
    <td>
      Python, PySpark, Kafka, Spark Structured Streaming, HDFS, Docker, bash
    </td>
  </tr>
  <tr>
    <td>
      5
    </td>
    <td>
      <a href="https://github.com/myshasolin/streaming_data_processing/tree/main/5%20Spark%20Streaming.%20Stateful%20streams">
        Spark Streaming. Stateful streams
      </a>
    </td>
    <td>
      файл <a href="https://github.com/myshasolin/streaming_data_processing/blob/main/5%20Spark%20Streaming.%20Stateful%20streams/exercise_films">exercise_films</a> - чтение топика в потоке с применением watermark и window, рассмотрены также разные режимы для вывода Output Modes (update, complete, append). <br> Дополнительно - чтение и объединение в потоке двух топиков
    </td>
    <td>
      PySpark, Kafka, Spark Structured Streaming, HDFS, Docker, bash
    </td>
  </tr>
  <tr>
    <td>
      6
    </td>
    <td>
      <a href="https://github.com/myshasolin/streaming_data_processing/tree/main/6%20Lambda%20%D0%B0%D1%80%D1%85%D0%B8%D1%82%D0%B5%D0%BA%D1%82%D1%83%D1%80%D0%B0.%20Spark%20Streaming%20%2B%20Cassandra">
        Lambda архитектура. Spark Streaming + Cassandra
      </a>
    </td>
    <td>
      два файла:<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/6%20Lambda%20%D0%B0%D1%80%D1%85%D0%B8%D1%82%D0%B5%D0%BA%D1%82%D1%83%D1%80%D0%B0.%20Spark%20Streaming%20%2B%20Cassandra/exercise_Harry_Potter">exercise_Harry_Potter</a> — команды из терминала (работа с кластером, БД Cassandra, Spark Streaming). Здесь всё по шагам, каждый ход комментирую, все комментарии оставил под # и постарался отделить их пробелами. Дополнительно при помощи cassandra-driver перегоняю csv в Cassandra<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/6%20Lambda%20%D0%B0%D1%80%D1%85%D0%B8%D1%82%D0%B5%D0%BA%D1%82%D1%83%D1%80%D0%B0.%20Spark%20Streaming%20%2B%20Cassandra/spark-submit_stream.py">spark-submit_stream.py</a> - скрипт для чтения csv командой spark-submit с трансформацией данных (добавлением столбца с датой) и из записбю на HDFS в parquet
    </td>
    <td>
      Python, PySpark, spark-submit, parquet, Kafka, Cassandra, CQL, Hive, HDFS, Docker, bash
    </td>
  </tr>
  <tr>
    <td>
      7
    </td>
    <td>
      <a href="https://github.com/myshasolin/streaming_data_processing/tree/main/7%20Spark%20ML.%20%D0%90%D0%BD%D0%B0%D0%BB%D0%B8%D1%82%D0%B8%D0%BA%D0%B0%20%D0%BF%D1%80%D0%B8%D0%B7%D0%BD%D0%B0%D0%BA%D0%BE%D0%B2%20%D0%B2%20%D0%BF%D0%B0%D0%BA%D0%B5%D1%82%D0%BD%D0%BE%D0%BC%20%D1%80%D0%B5%D0%B6%D0%B8%D0%BC%D0%B5.%20%D0%9F%D0%BE%D0%B4%D0%B3%D0%BE%D1%82%D0%BE%D0%B2%D0%BA%D0%B0%2C%20%D0%BE%D0%B1%D1%83%D1%87%D0%B5%D0%BD%D0%B8%D0%B5%20ML-%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8">
        Spark ML. Аналитика признаков в пакетном режиме. Подготовка, обучение ML-модели
      </a>
    </td>
    <td>
      три файла:<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/7%20Spark%20ML.%20%D0%90%D0%BD%D0%B0%D0%BB%D0%B8%D1%82%D0%B8%D0%BA%D0%B0%20%D0%BF%D1%80%D0%B8%D0%B7%D0%BD%D0%B0%D0%BA%D0%BE%D0%B2%20%D0%B2%20%D0%BF%D0%B0%D0%BA%D0%B5%D1%82%D0%BD%D0%BE%D0%BC%20%D1%80%D0%B5%D0%B6%D0%B8%D0%BC%D0%B5.%20%D0%9F%D0%BE%D0%B4%D0%B3%D0%BE%D1%82%D0%BE%D0%B2%D0%BA%D0%B0%2C%20%D0%BE%D0%B1%D1%83%D1%87%D0%B5%D0%BD%D0%B8%D0%B5%20ML-%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8/create_model.ipynb">create_model.ipynb</a> — здесь я создаю ML-модель и сохраняю её на HDFS<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/7%20Spark%20ML.%20%D0%90%D0%BD%D0%B0%D0%BB%D0%B8%D1%82%D0%B8%D0%BA%D0%B0%20%D0%BF%D1%80%D0%B8%D0%B7%D0%BD%D0%B0%D0%BA%D0%BE%D0%B2%20%D0%B2%20%D0%BF%D0%B0%D0%BA%D0%B5%D1%82%D0%BD%D0%BE%D0%BC%20%D1%80%D0%B5%D0%B6%D0%B8%D0%BC%D0%B5.%20%D0%9F%D0%BE%D0%B4%D0%B3%D0%BE%D1%82%D0%BE%D0%B2%D0%BA%D0%B0%2C%20%D0%BE%D0%B1%D1%83%D1%87%D0%B5%D0%BD%D0%B8%D0%B5%20ML-%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8/exercise_wine">exercise_wine</a> — здесь код из терминала. В нём я создаю топик, заливаю в него часть датасета, далее в стриме трансформирую батчи с применением сохранённой ранее модели, сначала вывожу несколько батчей в консоль, чтоб убедиться в том, что всё норм и ок, а потом запускаю стрим в таблицу в Cassandra. Все комментарии свои оставил за #<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/7%20Spark%20ML.%20%D0%90%D0%BD%D0%B0%D0%BB%D0%B8%D1%82%D0%B8%D0%BA%D0%B0%20%D0%BF%D1%80%D0%B8%D0%B7%D0%BD%D0%B0%D0%BA%D0%BE%D0%B2%20%D0%B2%20%D0%BF%D0%B0%D0%BA%D0%B5%D1%82%D0%BD%D0%BE%D0%BC%20%D1%80%D0%B5%D0%B6%D0%B8%D0%BC%D0%B5.%20%D0%9F%D0%BE%D0%B4%D0%B3%D0%BE%D1%82%D0%BE%D0%B2%D0%BA%D0%B0%2C%20%D0%BE%D0%B1%D1%83%D1%87%D0%B5%D0%BD%D0%B8%D0%B5%20ML-%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8/exercise_sql_ml">exercise_sql_ml</a> — соранил себе пример того, как делается синтетический датасет при помощи SQL с применением к нему потом ML
    </td>
    <td>
      Python, PySpark, pyspark.ml, Jupyter Notebook, Spark Structured Streaming, Kafka, Cassandra, HDFS, CQL, SQL, Hive, Docker, bash
    </td>
  </tr>
  <tr>
    <td>
      8
    </td>
    <td>
      <a href="https://github.com/myshasolin/streaming_data_processing/tree/main/8%20Spark%20Streaming%20%2B%20Spark%20ML%20%2B%20Cassandra.%20%D0%9F%D1%80%D0%B8%D0%BC%D0%B5%D0%BD%D0%B5%D0%BD%D0%B8%D0%B5%20ML-%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8%20%D0%B2%20%D1%80%D0%B5%D0%B6%D0%B8%D0%BC%D0%B5%20%D1%80%D0%B5%D0%B0%D0%BB%D1%8C%D0%BD%D0%BE%D0%B3%D0%BE%20%D0%B2%D1%80%D0%B5%D0%BC%D0%B5%D0%BD%D0%B8">
        Spark Streaming + Spark ML + Cassandra. Применение ML-модели в режиме реального времени
      </a>
    </td>
    <td>
      финальная работа, в ней 4 файла:<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/8%20Spark%20Streaming%20%2B%20Spark%20ML%20%2B%20Cassandra.%20%D0%9F%D1%80%D0%B8%D0%BC%D0%B5%D0%BD%D0%B5%D0%BD%D0%B8%D0%B5%20ML-%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8%20%D0%B2%20%D1%80%D0%B5%D0%B6%D0%B8%D0%BC%D0%B5%20%D1%80%D0%B5%D0%B0%D0%BB%D1%8C%D0%BD%D0%BE%D0%B3%D0%BE%20%D0%B2%D1%80%D0%B5%D0%BC%D0%B5%D0%BD%D0%B8/exercise_salary_prediction">exercise_salary_prediction</a> — здесь последовательные команды из терминала с комментариями шагов. Какие-то действия я выполнял в Cassandra, какие-то в Spark-сессии, а некоторые просто в окне терминала<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/8%20Spark%20Streaming%20%2B%20Spark%20ML%20%2B%20Cassandra.%20%D0%9F%D1%80%D0%B8%D0%BC%D0%B5%D0%BD%D0%B5%D0%BD%D0%B8%D0%B5%20ML-%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8%20%D0%B2%20%D1%80%D0%B5%D0%B6%D0%B8%D0%BC%D0%B5%20%D1%80%D0%B5%D0%B0%D0%BB%D1%8C%D0%BD%D0%BE%D0%B3%D0%BE%20%D0%B2%D1%80%D0%B5%D0%BC%D0%B5%D0%BD%D0%B8/create_model.ipynb">create_model.ipynb</a> — это скрипт создания пайплайна-модели с перебором сетки параметов и сохранения его в HDFS<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/8%20Spark%20Streaming%20%2B%20Spark%20ML%20%2B%20Cassandra.%20%D0%9F%D1%80%D0%B8%D0%BC%D0%B5%D0%BD%D0%B5%D0%BD%D0%B8%D0%B5%20ML-%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8%20%D0%B2%20%D1%80%D0%B5%D0%B6%D0%B8%D0%BC%D0%B5%20%D1%80%D0%B5%D0%B0%D0%BB%D1%8C%D0%BD%D0%BE%D0%B3%D0%BE%20%D0%B2%D1%80%D0%B5%D0%BC%D0%B5%D0%BD%D0%B8/get_submit.py">get_submit.py</a> — это скрипт дл запуска батчевого стриминга, в котором загружается обученная модель, объединяются и заполняются две таблицы из разных источников, из батча удаляется id, добавляется два новых столбца для предсказания, делается и добавляется это самое предсказание, потом удаляются все лишние столбцы и возвращается id. Готовый батч записывается в таблицу в Cassandra. Здесь обошёл ошибку, при которой foreachBatch любую модель воспринимал как int, при помощи вложенной функции process_batch в функцию foreach_batch_sink<br><br><a href="https://github.com/myshasolin/streaming_data_processing/blob/main/8%20Spark%20Streaming%20%2B%20Spark%20ML%20%2B%20Cassandra.%20%D0%9F%D1%80%D0%B8%D0%BC%D0%B5%D0%BD%D0%B5%D0%BD%D0%B8%D0%B5%20ML-%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D0%B8%20%D0%B2%20%D1%80%D0%B5%D0%B6%D0%B8%D0%BC%D0%B5%20%D1%80%D0%B5%D0%B0%D0%BB%D1%8C%D0%BD%D0%BE%D0%B3%D0%BE%20%D0%B2%D1%80%D0%B5%D0%BC%D0%B5%D0%BD%D0%B8/screen.png">screen.png</a> — это просто скрин экрана, показывающий два окна терминала — в левом при помощи spark-submit запущен стрим, а в правом открыта Cassandra и я в ней несколько раз выполняю один и то же SELECT-запрос, который показывает, как потихоньку по мере хода стрима таблица в Cassandra наполняется данными и столбцом с предсказанием
    </td>
    <td>
      Python, PySpark, pyspark.ml, Pipeline, ParamGridBuilder, spark-submit, Jupyter Notebook, Spark Structured Streaming, Kafka, Cassandra, HDFS, CQL, SQL, Hive, Docker, bash
    </td>
  </tr>
</table>
