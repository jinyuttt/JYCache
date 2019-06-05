using System;
using System.Collections.Generic;
using System.Text;

namespace CacheProvider
{
    class DataSerializer
    {

        /// <summary>
        /// 对象序列化
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static byte[] Serializer<T>(T obj)
        {
          return  MessagePack.MessagePackSerializer.Serialize(obj);
        }

        /// <summary>
        /// 对象反序列化
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="bytes"></param>
        /// <returns></returns>
        public static T Deserialize<T>(byte[] bytes)
        {
           return MessagePack.MessagePackSerializer.Deserialize<T>(bytes);
        }

        /// <summary>
        /// base64转换对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="base64"></param>
        /// <returns></returns>
        public static T DeserializeBase64<T>(string base64)
        {
           return Deserialize<T>(DecodeBase64(base64));
        }


        /// <summary>
        /// 对象转base64
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string SerializerBase64<T>(T obj)
        {
             return EncodeBase64(Serializer(obj));
        }



        private static string EncodeBase64(byte[] bytes)
        {
            return Convert.ToBase64String(bytes);
        }


        
        private static byte[] DecodeBase64(string code)
        {
            return Convert.FromBase64String(code);
            
        }
    }
}
