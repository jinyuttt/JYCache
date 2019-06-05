using System;

namespace BPlusTree
{
    /// <summary>
    /// Btree mapping unlimited length key strings to fixed length hash values
    /// </summary>
    public class HBplusTreeBytes : XBplusTreeBytes
    {
        public HBplusTreeBytes(BplusTreeBytes tree, int hashLength)
            : base(tree, hashLength)
        {
            // null out the culture context to use the naive comparison
            this.tree.NoCulture();
        }

        public new static HBplusTreeBytes Initialize(string treefileName, string blockfileName, int prefixLength, int cultureId,
            int nodesize, int buffersize)
        {
            return new HBplusTreeBytes(
                BplusTreeBytes.Initialize(treefileName, blockfileName, prefixLength, cultureId, nodesize, buffersize),
                prefixLength);
        }
        public new static HBplusTreeBytes Initialize(string treefileName, string blockfileName, int prefixLength, int cultureId)
        {
            return new HBplusTreeBytes(
                BplusTreeBytes.Initialize(treefileName, blockfileName, prefixLength, cultureId),
                prefixLength);
        }
        public new static HBplusTreeBytes Initialize(string treefileName, string blockfileName, int prefixLength)
        {
            return new HBplusTreeBytes(
                BplusTreeBytes.Initialize(treefileName, blockfileName, prefixLength),
                prefixLength);
        }
        public new static HBplusTreeBytes Initialize(System.IO.Stream treefile, System.IO.Stream blockfile, int prefixLength, int cultureId,
            int nodesize, int buffersize)
        {
            return new HBplusTreeBytes(
                BplusTreeBytes.Initialize(treefile, blockfile, prefixLength, cultureId, nodesize, buffersize),
                prefixLength);
        }
        public new static HBplusTreeBytes Initialize(System.IO.Stream treefile, System.IO.Stream blockfile, int prefixLength, int cultureId)
        {
            return new HBplusTreeBytes(
                BplusTreeBytes.Initialize(treefile, blockfile, prefixLength, cultureId),
                prefixLength);
        }
        public new static HBplusTreeBytes Initialize(System.IO.Stream treefile, System.IO.Stream blockfile, int prefixLength)
        {
            return new HBplusTreeBytes(
                BplusTreeBytes.Initialize(treefile, blockfile, prefixLength),
                prefixLength);
        }

        public new static HBplusTreeBytes ReOpen(System.IO.Stream treefile, System.IO.Stream blockfile)
        {
            BplusTreeBytes tree = BplusTreeBytes.ReOpen(treefile, blockfile);
            int prefixLength = tree.MaxKeyLength();
            return new HBplusTreeBytes(tree, prefixLength);
        }
        public new static HBplusTreeBytes ReOpen(string treefileName, string blockfileName)
        {
            BplusTreeBytes tree = BplusTreeBytes.ReOpen(treefileName, blockfileName);
            int prefixLength = tree.MaxKeyLength();
            return new HBplusTreeBytes(tree, prefixLength);
        }
        public new static HBplusTreeBytes ReadOnly(string treefileName, string blockfileName)
        {
            BplusTreeBytes tree = BplusTreeBytes.ReadOnly(treefileName, blockfileName);
            int prefixLength = tree.MaxKeyLength();
            return new HBplusTreeBytes(tree, prefixLength);
        }

        public override string PrefixForByteCount(string s, int maxbytecount)
        {
            byte[] inputbytes = BplusTree.StringToBytes(s);
            System.Security.Cryptography.MD5 D = System.Security.Cryptography.MD5.Create();
            byte[] digest = D.ComputeHash(inputbytes);
            byte[] resultbytes = new byte[maxbytecount];
            // copy digest translating to printable ascii
            for (int i = 0; i < maxbytecount; i++)
            {
                int r = digest[i % digest.Length];
                if (r > 127)
                {
                    r = 256 - r;
                }
                if (r < 0)
                {
                    r = -r;
                }
                //Console.WriteLine(" before "+i+" "+r);
                r = r % 79 + 40; // printable ascii
                //Console.WriteLine(" "+i+" "+r);
                resultbytes[i] = (byte)r;
            }
            string result = BplusTree.BytesToString(resultbytes);
            return result;
        }
        //		public override string PrefixForByteCount(string s, int maxbytecount)
        //		{
        //			// compute a hash code as a string which has maxbytecount size as a byte sequence
        //			byte[] resultbytes = new byte[maxbytecount];
        //			byte[] inputbytes = BplusTree.StringToBytes(s);
        //			int sevenbits = 127;
        //			int eighthbit = 128;
        //			bool invert = false;
        //			for (int i=0; i<maxbytecount; i++) 
        //			{
        //				resultbytes[i] = (byte) (i & sevenbits);
        //			}
        //			for (int i=0; i<inputbytes.Length; i++) 
        //			{
        //				int inputbyte = inputbytes[i];
        //				int outputindex = i % maxbytecount;
        //				int outputbyte = resultbytes[outputindex];
        //				int rotator = (i/maxbytecount) % 8;
        //				if (rotator!=0) 
        //				{
        //					int hipart = inputbyte << rotator;
        //					int lowpart = inputbyte >> (8-rotator);
        //					inputbyte = (hipart | lowpart);
        //				}
        //				outputbyte = ((inputbyte ^ outputbyte) % sevenbits);
        //				if ( (inputbyte&eighthbit)!=0 ) 
        //				{
        //					invert = !invert;
        //				}
        //				if (invert) 
        //				{
        //					outputbyte = (outputbyte ^ sevenbits) % eighthbit;
        //				}
        //				resultbytes[outputindex] = (byte) outputbyte;
        //			}
        //			string result = BplusTree.BytesToString(resultbytes);
        //			if (result.Length!=maxbytecount) 
        //			{
        //				throw new BplusTreeException("bad hash value generated with length: "+result.Length+" not "+maxbytecount);
        //			}
        //			return result;
        //		}
        public string ToHtml()
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            sb.Append(((BplusTreeBytes)this.tree).toHtml());
            sb.Append("\r\n<br><b>key / hash / value dump</b><br>");
            string currentkey = this.FirstKey();
            while (currentkey != null)
            {
                sb.Append("\r\n<br>" + currentkey);
                sb.Append(" / " + BplusNode.PrintableString(this.PrefixForByteCount(currentkey, this.PrefixLength)));
                try
                {
                    sb.Append(" / value found ");
                }
                catch (Exception)
                {
                    sb.Append(" !!!!!!! FAILED TO GET VALUE");
                }
                currentkey = this.NextKey(currentkey);
            }
            return sb.ToString();
        }
    }
}
