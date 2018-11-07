# JYCache
c#版本K-V缓存，实现FIFO,LRU,LFU策略。通过配置类设置缓存信息。
添加缓存元素时允许对单个元素设置缓存时间（秒），默认-1采用配置的缓存时间，设置0表示永久缓存，只对缓存最大约束有效，可以消失。其余时间则是缓存时间。

示例：
    ICache<int,int> cache=CacheFactory<int, int>.Create();//默认配置  
    cache.CacheRemoveListener += Cache_CacheRemoveListener;  
    cache.Add(1, 1);  
    或者  
    CacheConfig config = new CacheConfig();  
    config.CacheTime = 1800;//（时间都是秒) 
    config.Policy = CachePolicy.FIFO;//设置消失策略  
    ICache<int,int> cache=CacheFactory<int, int>.Create(config);  
    cache.CacheRemoveListener += Cache_CacheRemoveListener;  
    cache.Add(1, 1，10);//单独设置key保持10秒  
        
        
  
