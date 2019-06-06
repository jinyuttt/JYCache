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


-----------------------------------------------------------------------------------------

新增数据持久化  
   1.使用方法，创建时配置：config.PersPolicy = PersistencePolicy.Expire;
   2.如果获取数据，可以通过配置设置从持久化文件中获取：config.FindPers=true
   3.设置持久化刷新时间：config.PersTime=3000;//毫秒，默认值
   4.设置持久化数据目录：config.PersDir="KVData"；//默认值
   
  说明:当前只是实现，还没有严格测试，持久化采用B+树结构，注意，持久其实很慢，需要根据需要设置。
        
  
