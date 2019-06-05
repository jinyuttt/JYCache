namespace ICacheStore
{
    /// <summary>
    /// 持久化接口
    /// </summary>
    public interface ICacheStore
    {
         string Name { get; set; }

         string DataDir { get; set; }

        void Remove<K>(K key);

        void Add<K, V>(K key, V value);

        V Get<K, V>(K key);

        bool TryGetValue<K, V>(K key, out V value);

        void Commit();

        void ShutDown();
    }
}
