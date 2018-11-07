# JYCache
c#版本K-V缓存，实现FIFO,LRU,LFU策略。通过配置类设置缓存信息。
添加缓存元素时允许对单个元素设置缓存时间（秒），默认-1采用配置的缓存时间，设置0表示永久缓存，只对缓存最大约束有效，可以消失。其余时间则是缓存时间。

示例：
    ICache<int,int> cache=CacheFactory<int, int>.Create();//默认配置 \r\n
    cache.CacheRemoveListener += Cache_CacheRemoveListener;\r\n
    cache.Add(1, 1);\r\n
    或者\r\n
    CacheConfig config = new CacheConfig();\r\n
    config.CacheTime = 1800;//（时间都是秒）\r\n
    config.Policy = CachePolicy.FIFO;//设置消失策略\r\n
    ICache<int,int> cache=CacheFactory<int, int>.Create(config);\r\n
    cache.CacheRemoveListener += Cache_CacheRemoveListener;\r\n
    cache.Add(1, 1，10);//单独设置key保持10秒\r\n
        
        
  
