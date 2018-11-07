using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.IO;
using System.Reflection;
using System.Text;

/**
* 命名空间: JYCache 
* 类 名： CacheConfig
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace JYCache
{
    /// <summary>
    /// 功能描述    ：CacheConfig  缓存配置
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/11/5 0:09:28 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/11/5 0:09:28 
    /// </summary>
   public class CacheConfig
    {

        private int initSize = 100;
        private int concurrencyLevel = -1;
        private float weigher = 0.9f;
        private int cacheTime = 300;
        private int maxCacheSize = int.MaxValue;
        /// <summary>
        /// 最大缓存
        /// </summary>
        public int MaxCacheSize { get { return maxCacheSize; } set { maxCacheSize = value; } }

        /// <summary>
        /// 缓存时间(秒)
        /// 默认0永久有效
        /// </summary>
        public int CacheTime { get { return cacheTime; } set { cacheTime = value; } }

        /// <summary>
        ///移除策略
        /// </summary>
        public CachePolicy Policy { get; set; }

        /// <summary>
        /// 初始化大小
        /// 默认100
        /// </summary>
        public int InitSzie { get { return initSize; } set { initSize = value; } }

        /// <summary>
        /// 弱key移除
        /// </summary>
        public bool WeakKey { get; set; }

        /// <summary>
        /// 弱value移除
        /// </summary>
        public bool WeakValue { get; set; }

        /// <summary>
        /// 访问线程数
        /// 默认：CPU*2
        /// </summary>
        public int ConcurrencyLevel { get { return concurrencyLevel; } set { concurrencyLevel = value; } }

        /// <summary>
        /// 权重
        /// 默认：0.9
        /// </summary>
        public float Weight { get { return weigher; } set { weigher = value; Contract.Assert(weigher > 1); } }


        /// <summary>
        /// 可以利用空间换时间
        /// 记操作
        /// </summary>
        public bool UseMemory { get; set; }

        /// <summary>
        /// 加载配置文件
        /// </summary>
        /// <param name="file"></param>
        public void LoadConfig(string file)
        {
            Dictionary<string, string> dic = new Dictionary<string, string>();
            if(File.Exists(file))
            {
                string line = null;
                string[] cfg = null;
                using (StreamReader rd = new StreamReader(file))
                {
                    while(null!=(line=rd.ReadLine()))
                    {
                        cfg = line.Split('=');
                        if(cfg.Length==2)
                        {
                            string name = cfg[0];
                            string value = cfg[1];
                            if(!string.IsNullOrEmpty(name)&&!string.IsNullOrEmpty(value))
                            {
                                dic[name.ToLower().Trim()] = value.Trim();
                            }
                        }
                    }
                }
                //
               var properties= this.GetType().GetProperties();
                string pvalue = null;
                foreach (PropertyInfo pinf in properties)
                {
                    string name = pinf.Name.ToLower();
                    if (dic.TryGetValue(name, out pvalue))
                    {
                        pinf.SetValue(this, Convert.ChangeType(pvalue, pinf.PropertyType));
                    }
                }
            }
        }
    }
}
