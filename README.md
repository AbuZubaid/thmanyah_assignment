*تكليف ثمانية* 

نظامٌ تم تصميمه كـ تكليف من ثمانية، يقوم ببث فعاليات تفاعل المستخدمين من قاعدة بيانات PostgreSQL إلى ثلاث وجهات مختلفة — Clickhouse (منصة تحليل بيانات عمودية) كبديل عن BigQuery، و Redis ، ونظام آخر (jsonl) — مع إجراء عمليات تحويل (Transformation) وإثراء (Enrichment) على البيانات.

_________________________________________________________
*التقنيات المستخدمة* 

PostgreSQL, Debezium, Apache Kafka, Kafka Connect, Kafka Streams (Java), and Docker.

__________________________________________________________

*التصميم* 

![B563AA0B-6B34-484F-BC11-CFAD09B8B890_1_201_a](https://github.com/user-attachments/assets/b77846ba-dda4-42d5-ae54-5b76ae51ac15)


_________________________________________________________________________
*متطلبات التشغيل* 

Git (version control system)

Java, version 17 or higher

Docker Desktop

Maven v3.x

Python 3.1x

________________________________________________________________


*خطوات التشغيل*

1- قم بعمل clone  لهذا المستودع، أو قم بتنزيله كـ ZIP file. 
2- تأكد من تشغيل  Docker Desktop

3- شغل الـ Services الرئيسية من خلال فتح terminal على عنوان المشروع، وتشغيل الأمر التالي: 


docker-compose up --build -d zookeeper kafka postgresql kafka-connect redis clickhouse


4- انتظر دقيقة، ثم شغل الأمر التالي:


( curl -X POST -H "Content-Type: application/json" \
--data @connectors/postgres-connector.json \
http://localhost:8084/connectors )


4- قم بتوليد البيانات من خلال سكريبت بايثون data_generator.py


5- تأكد أن كلاً من mydb.public.content و mydb.public.engagement_events موجودين في القائمة عند تشغيل الأمر التالي:


docker exec -it kafka kafka-topics \
--bootstrap-server kafka:29092 --list

6- قم بتشغيل الأمر التالي لإضافة باقي الـ services : 



docker-compose up -d --no-deps enrichment-app redis-sink jsonl-sink ch-sink


7- تأكد من الـ Consuming وأن كافكا يرى البيانات، من خلال تشغيل الأمرين التاليين: 




( docker exec -it kafka kafka-console-consumer \
--bootstrap-server kafka:29092 \
--topic mydb.public.content \
--from-beginning --max-messages 3 \
--property print.key=true --property key.separator=" | " )



( docker exec -it kafka kafka-console-consumer \
--bootstrap-server kafka:29092 \
--topic mydb.public.engagement_events \
--from-beginning --max-messages 3 \
--property print.key=true --property key.separator=" | " )



8- تأكد من وصول البيانات المثراة إلى الـ sinks من خلال الأوامر التالية:


( docker exec -it kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic enriched.clickhouse \
    --from-beginning --property print.key=true --property key.separator=" | " )

    

( docker exec -it kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic enriched.jsonl \
    --from-beginning --property print.key=true --property key.separator=" | " )
    

( docker exec -it kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic enriched.redis \
    --from-beginning --property print.key=true --property key.separator=" | " )
    

او مثلا


 docker exec -it redis redis-cli


SCAN 0

GET 1


بامكانك فتح ملف json-data/enriched.json لترى البيانات التي تم تخزينها محليا. 


كل المودة،

محمد ابو زبيد
