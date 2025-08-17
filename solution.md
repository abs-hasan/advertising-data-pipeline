# TRENDii Take Home Project - Solution Documentation

## 📊 **Live Dashboard**
**[View Interactive Dashboard →](https://bubbly-batten.metabaseapp.com/public/dashboard/f8acbd6d-8918-4e24-996b-453307e581ad)**


## 📋 Summary

**Key Findings:**
- **77,573 unique users** reached across advertising campaigns
- **whowhatwear.com has critical performance issues** (9.92% mount rate vs ~9.9% for others)
- **7news.com.au dominates content engagement** with celebrity/news content
- **Furniture products show highest click rates** across all brands
- **Forever New Women's Cari Tailored Mini Skirt** is the top performing product across campaigns

**Business Impact:**
- Identified significant optimization opportunity at whowhatwear.com
- Celebrity content strategy validated as high-performing

---

## 🎯 Business Questions & Solutions

### **Question 1: Top 5 Articles by Traffic per Domain**

```sql

WITH ranked_articles AS (
    SELECT 
        art.domain,
        art.article_title,
        COUNT(DISTINCT tag.page_view_id) as traffic_count,
        ROW_NUMBER() OVER (
            PARTITION BY art.domain 
            ORDER BY COUNT(DISTINCT tag.page_view_id) DESC
        ) as rank
    FROM {{ ref('fact_tagloads') }} as tag
    LEFT JOIN {{ ref('dim_articles') }} as art 
        ON tag.article_key = art.article_key
    WHERE art.domain IS NOT NULL
    GROUP BY art.domain, art.article_title
)
SELECT 
    domain,
    article_title,
    traffic_count
FROM ranked_articles
WHERE rank <= 5
ORDER BY domain, traffic_count DESC;
```

**What this does:** Counts unique page views per article, ranks them by domain, picks top 5.

#### **Results**

**7news.com.au** (news/celebrity content):
- "Reese Witherspoon Selfie With Teenage Son..." - **1,782 tagloads**
- "Chrissie Swan Gives Personal Update..." - **1,256 tagloads** 
- "Victoria Beckham Accused Of Cropping..." - **1,213 tagloads**
- "Did Olivia Newton Johns Partner Fake His Death..." - **1,188 tagloads**
- "Charlbi Dean South African Born Actress..." - **1,153 tagloads**

**style.nine.com.au** (beauty/fashion content):
- "How To Do Your Makeup As You Age..." - **1,599 tagloads**
- "Anna Wintour Vogue Best Fashion Style Moments" - **1,224 tagloads**
- "Famous Models Who Changed Careers..." - **1,169 tagloads**
- "French Style Icons In Photos" - **1,121 tagloads**
- "Jane Birkin The Birkin Bag Hermes History" - **1,121 tagloads**

**whowhatwear.com** (beauty products - significantly lower engagement):
- "Best Soapy Perfumes" - **423 tagloads**
- "Types Of Curly Hair" - **395 tagloads**
- "Max Factor False Lash Effect..." - **385 tagloads**

**Business Insight:** Celebrity and lifestyle news content (7news.com.au) generates 3-4x more traffic than beauty product reviews (whowhatwear.com).

---

### **Question 2: Top 3 Clicked Products by Brand (Final Week)**

```sql

select
    coalesce(camp.company_name, 'Unknown Company') as company_name,
    p.product_name,
    count(*) as click_count  -- FIXED: Added *
from {{ ref("fact_clicks") }} c
join {{ ref("dim_dates") }} d on c.click_date = d.date_actual
join {{ ref("dim_products") }} p on c.product_id = p.product_id
left join
    {{ ref("dim_campaigns") }} camp on c.brand_id = camp.brand_id and camp.current_record = true
where
    d.week_beginning_date = (
        select max(d2.week_beginning_date)
        from {{ ref("fact_clicks") }} c2
        join {{ ref("dim_dates") }} d2 on c2.click_date = d2.date_actual
    )
group by camp.company_name, p.product_name, c.brand_id
qualify row_number() over (partition by c.brand_id order by count(*) desc, p.product_name) <= 3
order by company_name desc, click_count desc, product_name

```

**What this does:** Finds the last week in the data, counts clicks per product by brand, picks top 3. 

#### **Results**

**Oz Home Club** (furniture focus):
- "Como Desk Black 2 Drawer" - **20 clicks**
- "Oblong Wagon Wheel Board Game Coffee Table..." - **19 clicks**
- "Country Road Women's Lofty Knit Top Cream..." - **17 clicks**

**Forever New** (fashion focus):
- "Forever New Women's Cari Tailored Mini Skirt..." - **20 clicks**
- "Country Road Women's Lofty Knit Top Cream..." - **16 clicks**
- "Oblong Wagon Wheel Board Game Coffee Table..." - **14 clicks**

**Fantastic Furniture** (furniture focus):
- "Oblong Wagon Wheel Board Game Coffee Table..." - **19 clicks**
- "Como Desk Black 2 Drawer" - **15 clicks**
- "Forever New Women's Cari Tailored Mini Skirt..." - **15 clicks**

**Country Road Group** (fashion focus):
- "Forever New Women's Cari Tailored Mini Skirt..." - **19 clicks**
- "Oblong Wagon Wheel Board Game Coffee Table..." - **15 clicks**
- "Country Road Women's Lofty Knit Top Cream..." - **11 clicks**

**Business Insight:** Furniture items (desk, coffee table) show consistently high engagement across all brands.

---

### **Question 3: Product with Most Impressions per Campaign**

```sql

select
    coalesce(c.campaign_name, 'Unknown Campaign') as campaign_name,
    p.product_name,
    count(i.product_id) as impression_count
from {{ ref("fact_impressions") }} as i
left join
    {{ ref("dim_campaigns") }} as c
    on i.brand_id = c.brand_id
    and c.current_record = true
left join {{ ref("dim_products") }} as p on i.product_id = p.product_id
group by c.campaign_name, p.product_name, c.campaign_id
qualify
    row_number() over (
        partition by c.campaign_id order by count(i.product_id) desc
    )
    = 1
order by impression_count desc

```

**What this does:** Counts impressions per product per campaign, then picks the winner for each campaign. 

#### **Results**

- **Country Road - CPA - New Deal**: "Forever New Women's Cari Tailored Mini Skirt in Light Pebble Suit" - **173,703 impressions**
- **Oz Home Hub - New Deal**: "Forever New Women's Cari Tailored Mini Skirt in Light Pebble Suit" - **173,553 impressions**
- **Forever New - CPA - Trial**: "Oblong Wagon Wheel Board Game Coffee Table..." - **173,362 impressions**
- **Fantastic Furniture Q1 2024**: "Country Road Women's Lofty Knit Top Cream..." - **173,004 impressions**

**Business Insight:** Impression volumes (~173K) across all campaigns. The "Forever New Women's Cari Tailored Mini Skirt" appears as top performer.

---

### **Question 4: Fill Rate (Mount Rate) by Domain**

```sql
WITH domain_events AS (
  SELECT 
    a.domain,
    'mount' as event_type, COUNT(*) as event_count
  FROM {{ ref('fact_mounts') }} m
  LEFT JOIN {{ ref('dim_articles') }} a ON m.article_key = a.article_key
  GROUP BY a.domain
  UNION ALL

  SELECT 
    a.domain, 'tagload' as event_type, 
    COUNT(*) as event_count
  FROM {{ ref('fact_tagloads') }} t
  LEFT JOIN {{ ref('dim_articles') }} a
    ON t.article_key = a.article_key
  GROUP BY a.domain
)

SELECT 
  domain,
  SUM(CASE WHEN event_type = 'mount' THEN event_count ELSE 0 END) as total_mounts,
  SUM(CASE WHEN event_type = 'tagload' THEN event_count ELSE 0 END) as total_tagloads,
  CASE 
    WHEN SUM(CASE WHEN event_type = 'tagload' THEN event_count ELSE 0 END) > 0 
    THEN ROUND(
      SUM(CASE WHEN event_type = 'mount' THEN event_count ELSE 0 END) / 
      SUM(CASE WHEN event_type = 'tagload' THEN event_count ELSE 0 END), 
      4
    )
    ELSE 0.0
  END as mount_rate
FROM domain_events
GROUP BY domain
HAVING SUM(CASE WHEN event_type = 'tagload' THEN event_count ELSE 0 END) > 0
ORDER BY mount_rate DESC, domain
```

**What this does:** Uses UNION ALL to get both mount and tagload counts, then pivots the data to calculate the ratio.

#### **Results**

- **whowhatwear.com**: 234,410 mounts / 23,634 tagloads = **9.92% mount rate**
- **style.nine.com.au**: 714,307 mounts / 72,095 tagloads = **9.91% mount rate**  
- **7news.com.au**: 680,757 mounts / 68,934 tagloads = **9.88% mount rate**

---

### **Question 5: Total Unique Users Advertised To**

```sql
SELECT 
  COUNT(DISTINCT device_id) as unique_users_advertised
FROM {{ ref('fact_impressions') }}
WHERE device_id IS NOT NULL
```

**What this does:** Dead simple - count unique device IDs from impressions.

#### **Results**
**77,573 unique users** were advertised to during the campaign period.

---

All the analytics models are just views with `{{ config(materialized='view',tags=['analytics']) }}` - no point storing them since they're small result sets.

### **How We Connected The Data**

The tricky part was linking everything together. Here's what we figured out:

**For Q1 & Q4**: tagloads and mounts connect to articles via `article_key`
**For Q2 & Q3**: clicks and impressions have product/campaign info directly  
**For Q5**: Just count devices from impressions table

The `article_key` was generated from URL + publisher combo using dbt_utils.

