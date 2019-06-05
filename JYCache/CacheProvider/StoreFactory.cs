using BPlusTree;
using System;
using System.IO;

namespace CacheProvider
{

    /// <summary>
    /// 分2类保存数据
    /// </summary>
    public class CacheStore : ICacheStore.ICacheStore
    {
        private const string TreeDataDir ="KVData";
        private const int DefaultNodeSize = 2048;
        private const int DefaultKeyLength = 64;
        private BplusTreeBytes _treeProperty;
        private  HBplusTreeBytes treeXbBytes;
     
        private readonly int LcId = System.Globalization.CultureInfo.InvariantCulture.LCID;

        public string Name { get; set; }

        public string DataDir { get; set; }

        private BplusTreeBytes TreeProperty
        {
            get
            {
                var treeFilePath = Path.Combine(DataDir, TreeLongFileName);
                if (_treeProperty == null)
                {
                    if (File.Exists(treeFilePath))
                    {
                        Console.WriteLine("Re-Opent A Existing Tree(Maybe The App Had An Restarting)");
                        _treeProperty = BplusTreeBytes.ReOpen(
                           TreeLongFileName, ValueLongFileName
                            );

                        return _treeProperty;
                    }

                    Console.WriteLine("Create A New Tree In The First Time");
                    _treeProperty = BplusTreeBytes.Initialize(
                       TreeLongFileName, ValueLongFileName, DefaultKeyLength,LcId
                        );
                    return _treeProperty;
                }
                return _treeProperty;
            }
        }

        private HBplusTreeBytes HBplusTree
        {
            get
            {
                var treeFilePath = Path.Combine(DataDir, TreeFileName);
                var valueFilePath = Path.Combine(DataDir, ValueFileName);
                if (treeXbBytes == null)
                {
                    if (File.Exists(treeFilePath))
                    {
                        Console.WriteLine("Re-Opent A Existing Tree(Maybe The App Had An Restarting)");
                        treeXbBytes = HBplusTreeBytes.ReOpen(

                            treeFilePath, valueFilePath
                            );

                        return treeXbBytes;
                    }

                    Console.WriteLine("Create A New Tree In The First Time");
                    treeXbBytes = HBplusTreeBytes.Initialize(
                        treeFilePath, valueFilePath, DefaultKeyLength, LcId
                      );
                    return treeXbBytes;
                }


                return treeXbBytes;
            }
        }

        public CacheStore()
        {
            this.DataDir = TreeDataDir;
        }

        private string TreeFileName
        {
            get
            {
                return string.Format("KeyFile_{0}.key" ,Name);
            }
        }

        private  string ValueFileName
        {
            get
            {
                return string.Format("ValueFile_{0}.key", Name);
            }
        }

        private string TreeLongFileName
        {
            get
            {
                return string.Format("KeyLongFile_{0}.key", Name);
            }
        }

        private string ValueLongFileName
        {
            get
            {
                return string.Format("ValueLongFile_{0}.key", Name);
            }
        }

        public void Add<K, V>(K key, V value)
        {
            if(key is int || key is short || key is uint || key is ushort|| key is long ||key is ulong )
            {
                TreeProperty[key.ToString()] = DataSerializer.Serializer(value);
            }
            else
            {
               HBplusTree[DataSerializer.SerializerBase64(key)]= DataSerializer.Serializer(value);
            }
        }

        public void Commit()
        {
           if(treeXbBytes!=null)
            {
                treeXbBytes.Commit();
            }
           if(_treeProperty!=null)
            {
                _treeProperty.Commit();
            }
        }

        public void Remove<K>(K key)
        {
            if (key is int || key is short || key is uint || key is ushort || key is long || key is ulong)
            {
                TreeProperty.RemoveKey(key.ToString());
            }
            else
            {
                HBplusTree.RemoveKey(DataSerializer.SerializerBase64(key));
            }
        }

        public void ShutDown()
        {
            if(treeXbBytes!=null)
            {
                treeXbBytes.Shutdown();
            }
            if(_treeProperty!=null)
            {
                _treeProperty.Shutdown();
            }
        }

        public V Get<K, V>(K key)
        {
            if (_treeProperty != null)
            {
                var result = _treeProperty.Get(key.ToString(),null) as byte[];
                if(result!=null)
                {
                    return DataSerializer.Deserialize<V>(result);
                }

            }
            if(treeXbBytes!=null)
            {
                var result = treeXbBytes.Get(key.ToString(), null) as byte[];
                if (result != null)
                {
                    return DataSerializer.Deserialize<V>(result);
                }
            }
            return default(V);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool TryGetValue<K, V>(K key, out V value)
        {
            bool isFind = false;
            value = default(V);
            if (_treeProperty != null)
            {
                var result = _treeProperty.Get(key.ToString(), null) as byte[];
                if (result != null)
                {
                    value= DataSerializer.Deserialize<V>(result);
                    isFind = true;
                }

            }
            if (treeXbBytes != null)
            {
                var result = treeXbBytes.Get(key.ToString(), null) as byte[];
                if (result != null)
                {
                    value = DataSerializer.Deserialize<V>(result);
                    isFind = true;
                }
            }
            return isFind;
        }
    }
}
