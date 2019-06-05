namespace JYCache
{

    /// <summary>
    /// 持久化策略
    /// </summary>
    public enum PersistencePolicy
    {

        /// <summary>
        /// 不持久化
        /// </summary>
        None,

        /// <summary>
        /// 过期缓存的数据持久化
        /// </summary>
        Expire,

        /// <summary>
        /// 缓存所有数据持久
        /// </summary>
        All,

        /// <summary>
        /// 数据同步
        /// </summary>
        Synchro
    }
}
