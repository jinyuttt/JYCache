using JYCache;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {

            CacheConfig config = new CacheConfig();
            config.CacheTime = 1800;
            config.Policy = CachePolicy.FIFO;
           ICache<int,int> cache=CacheFactory<int, int>.Create(config);
            cache.CacheRemoveListener += Cache_CacheRemoveListener;
            
            for(int i=0;i<10;i++)
            {
                Task.Factory.StartNew(() =>
                {
                    int num = i * 10;
                    for (int j = num; j <num+10000; j++)
                    {
                        cache.Add(j, j);
                        cache.Add()
                    }
                });
            }
            Thread.Sleep(10000);
            int reslut = 0;
            if(cache.TryGetCache(10020,out reslut))
            {
                Console.WriteLine(reslut);
            }
            Console.Read();

        }

        private static void Cache_CacheRemoveListener(object sender, int key, int value, int state)
        {
            Console.WriteLine(key);
        }
    }
}
