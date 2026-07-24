-- Andariya Monitoring Data - SQL Queries for Tableau
-- Connect Tableau to PostgreSQL and use these queries as Custom SQL

-- ============================================================
-- 1. OVERVIEW DASHBOARD: Post distribution by platform
-- ============================================================
SELECT 
    platform,
    COUNT(*) as post_count,
    COUNT(DISTINCT actor_category) as unique_actors,
    COUNT(DISTINCT disarm_tactic) as unique_tactics
FROM clean_posts
GROUP BY platform
ORDER BY post_count DESC;

-- ============================================================
-- 2. ACTOR ANALYSIS: Who is producing content?
-- ============================================================
SELECT 
    actor_category,
    platform,
    COUNT(*) as post_count,
    ROUND(AVG(degree_numeric)::numeric, 0) as avg_engagement
FROM clean_posts
WHERE actor_category IS NOT NULL
GROUP BY actor_category, platform
ORDER BY post_count DESC;

-- ============================================================
-- 3. DISARM FRAMEWORK: Tactics and techniques breakdown
-- ============================================================
SELECT 
    disarm_tactic,
    disarm_technique,
    verification_status,
    COUNT(*) as occurrence_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY disarm_tactic), 1) as pct_of_tactic
FROM clean_posts
WHERE disarm_tactic IS NOT NULL
GROUP BY disarm_tactic, disarm_technique, verification_status
ORDER BY disarm_tactic, occurrence_count DESC;

-- ============================================================
-- 4. NARRATIVE TRACKING: Primary narratives over time
-- ============================================================
SELECT 
    date_collected,
    primary_narrative,
    COUNT(*) as post_count,
    STRING_AGG(DISTINCT platform, ', ') as platforms
FROM clean_posts
WHERE date_collected IS NOT NULL
GROUP BY date_collected, primary_narrative
ORDER BY date_collected DESC, post_count DESC;

-- ============================================================
-- 5. VERIFICATION STATUS: Information reliability
-- ============================================================
SELECT 
    verification_status,
    COUNT(*) as post_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM clean_posts), 1) as percentage
FROM clean_posts
GROUP BY verification_status
ORDER BY post_count DESC;

-- ============================================================
-- 6. CONTENT TYPE & ENGAGEMENT: What formats are used?
-- ============================================================
SELECT 
    content_type,
    degree_qualitative,
    COUNT(*) as post_count,
    ROUND(AVG(degree_numeric)::numeric, 0) as avg_numeric_engagement
FROM clean_posts
GROUP BY content_type, degree_qualitative
ORDER BY content_type, avg_numeric_engagement DESC NULLS LAST;

-- ============================================================
-- 7. EFFECT ANALYSIS: Impact assessment
-- ============================================================
SELECT 
    effect_category,
    COUNT(*) as post_count,
    STRING_AGG(DISTINCT primary_narrative, ' | ') as related_narratives
FROM clean_posts
WHERE effect_category IS NOT NULL
GROUP BY effect_category
ORDER BY post_count DESC;

-- ============================================================
-- 8. COMPLETE DATASET: All fields for detailed analysis
-- ============================================================
SELECT 
    id,
    platform,
    date_collected,
    url,
    actor_category,
    behaviour_code,
    content_type,
    degree_numeric,
    degree_qualitative,
    effect_category,
    primary_narrative,
    disarm_tactic,
    disarm_technique,
    verification_status,
    notes,
    cleaned_at
FROM clean_posts
ORDER BY date_collected DESC, id DESC;