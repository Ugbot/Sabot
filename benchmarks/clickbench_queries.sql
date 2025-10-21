-- ClickBench SQL Queries
-- https://github.com/ClickHouse/ClickBench
-- Standard SQL benchmark for analytical databases

-- Dataset: hits table (100M rows, web analytics data)
-- Columns: WatchID, JavaEnable, Title, GoodEvent, EventTime, EventDate, CounterID, 
--          ClientIP, RegionID, UserID, CounterClass, OS, UserAgent, URL, Referer,
--          IsRefresh, RefererCategoryID, RefererRegionID, URLCategoryID, URLRegionID,
--          ResolutionWidth, ResolutionHeight, ResolutionDepth, FlashMajor, FlashMinor,
--          FlashMinor2, NetMajor, NetMinor, UserAgentMajor, UserAgentMinor, CookieEnable,
--          JavascriptEnable, IsMobile, MobilePhone, MobilePhoneModel, Params, IPNetworkID,
--          TraficSourceID, SearchEngineID, SearchPhrase, AdvEngineID, IsArtifical,
--          WindowClientWidth, WindowClientHeight, ClientTimeZone, ClientEventTime,
--          SilverlightVersion1, SilverlightVersion2, SilverlightVersion3, SilverlightVersion4,
--          PageCharset, CodeVersion, IsLink, IsDownload, IsNotBounce, FUniqID, OriginalURL,
--          HID, IsOldCounter, IsEvent, IsParameter, DontCountHits, WithHash, HitColor,
--          LocalEventTime, Age, Sex, Income, Interests, Robotness, RemoteIP, WindowName,
--          OpenerName, HistoryLength, BrowserLanguage, BrowserCountry, SocialNetwork,
--          SocialAction, HTTPError, SendTiming, DNSTiming, ConnectTiming, ResponseStartTiming,
--          ResponseEndTiming, FetchTiming, SocialSourceNetworkID, SocialSourcePage,
--          ParamPrice, ParamOrderID, ParamCurrency, ParamCurrencyID, OpenstatServiceName,
--          OpenstatCampaignID, OpenstatAdID, OpenstatSourceID, UTMSource, UTMMedium,
--          UTMCampaign, UTMContent, UTMTerm, FromTag, HasGCLID, RefererHash, URLHash,
--          CLID

-- Query 0: Simple count
SELECT COUNT(*) FROM hits;

-- Query 1: Simple aggregation
SELECT COUNT(*) FROM hits WHERE AdvEngineID != 0;

-- Query 2: Group by aggregation
SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;

-- Query 3: Simple aggregate with filter
SELECT SUM(UserID) FROM hits;

-- Query 4: Complex filter
SELECT COUNT(DISTINCT UserID) FROM hits;

-- Query 5: Top N with group by
SELECT CounterID, COUNT(*) AS c FROM hits GROUP BY CounterID ORDER BY c DESC LIMIT 10;

-- Query 6: Simple group by
SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;

-- Query 7: Top regions by page views
SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) 
FROM hits 
GROUP BY RegionID 
ORDER BY c DESC 
LIMIT 10;

-- Query 8: Complex aggregation
SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u 
FROM hits 
WHERE MobilePhoneModel != '' 
GROUP BY MobilePhoneModel 
ORDER BY u DESC 
LIMIT 10;

-- Query 9: String search
SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u 
FROM hits 
WHERE MobilePhoneModel != '' 
GROUP BY MobilePhone, MobilePhoneModel 
ORDER BY u DESC 
LIMIT 10;

-- Query 10: Complex search and aggregation  
SELECT SearchPhrase, COUNT(*) AS c 
FROM hits 
WHERE SearchPhrase != '' 
GROUP BY SearchPhrase 
ORDER BY c DESC 
LIMIT 10;

