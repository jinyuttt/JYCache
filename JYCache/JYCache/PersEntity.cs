namespace JYCache
{
    internal class PersEntity<TKey,TValue>
    {
        public PersEntity(TKey key,TValue value)
        {
            Key = key;
            Value = value;
        }
        public TKey Key { get; set; }

        public TValue Value { get; set; }
    }
}
