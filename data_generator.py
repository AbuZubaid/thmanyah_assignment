import psycopg2
import uuid
import random
import json
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

conn = psycopg2.connect(
    dbname="mydb",
    user="user",
    password="password",
    host="localhost",   
    port="5432"         
)

cur = conn.cursor()

# إنشاء محتويات (content)
def insert_content(n=5):
    contents = []
    for _ in range(n):
        cid = str(uuid.uuid4())
        slug = fake.slug()
        title = fake.sentence(nb_words=3)
        ctype = random.choice(["podcast", "newsletter", "video"])
        length = random.randint(60, 600)  # من 1 دقيقة إلى 10 دقائق
        publish_ts = datetime.utcnow() - timedelta(days=random.randint(0, 30))

        cur.execute("""
            INSERT INTO content (id, slug, title, content_type, length_seconds, publish_ts)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (slug) DO NOTHING
        """, (cid, slug, title, ctype, length, publish_ts))

        contents.append((cid, title))
    return contents

# إنشاء تفاعلات (engagement_events)
def insert_engagement_events(contents, n=20):
    for _ in range(n):
        content_id, _ = random.choice(contents)
        user_id = str(uuid.uuid4())
        event_type = random.choice(["play", "pause", "finish", "click"])
        event_ts = datetime.utcnow()
        duration_ms = random.choice([None, random.randint(1000, 300000)])  # 1s إلى 5min
        device = random.choice(["ios", "android", "web-safari", "web-chrome"])
        raw_payload = {"session": fake.uuid4(), "extra": fake.word()}

        cur.execute("""
            INSERT INTO engagement_events (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (content_id, user_id, event_type, event_ts, duration_ms, device, json.dumps(raw_payload)))

# تشغيل التوليد
if __name__ == "__main__":
    contents = insert_content(5)      # 5 محتويات
    insert_engagement_events(contents, 30)  # 30 تفاعل

    conn.commit()
    print("✅ تم إدخال بيانات تجريبية بنجاح!")

    cur.close()
    conn.close()
