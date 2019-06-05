namespace BPlusTree
{
    /// <summary>
    /// Tree index mapping strings to strings with unlimited key length
    /// </summary>
    public class XBplusTree : BplusTree
    {
        XBplusTreeBytes xtree;
        public XBplusTree(XBplusTreeBytes tree)
            : base(tree)
        {
            this.xtree = tree;
        }
        protected override bool CheckTree()
        {
            return false;
        }
        public void LimitBucketSize(int limit)
        {
            this.xtree.BucketSizeLimit = limit;
        }
        public static new XBplusTree Initialize(string treefileName, string blockfileName, int PrefixLength, int CultureId,
            int nodesize, int buffersize)
        {
            XBplusTreeBytes tree = XBplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength, CultureId, nodesize, buffersize);
            return new XBplusTree(tree);
        }
        public static new XBplusTree Initialize(string treefileName, string blockfileName, int PrefixLength, int CultureId)
        {
            XBplusTreeBytes tree = XBplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength, CultureId);
            return new XBplusTree(tree);
        }
        public static new XBplusTree Initialize(string treefileName, string blockfileName, int PrefixLength)
        {
            XBplusTreeBytes tree = XBplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength);
            return new XBplusTree(tree);
        }

        public static new XBplusTree Initialize(System.IO.Stream treefile, System.IO.Stream blockfile, int PrefixLength, int CultureId,
            int nodesize, int buffersize)
        {
            XBplusTreeBytes tree = XBplusTreeBytes.Initialize(treefile, blockfile, PrefixLength, CultureId, nodesize, buffersize);
            return new XBplusTree(tree);
        }
        public static new XBplusTree Initialize(System.IO.Stream treefile, System.IO.Stream blockfile, int PrefixLength, int CultureId)
        {
            XBplusTreeBytes tree = XBplusTreeBytes.Initialize(treefile, blockfile, PrefixLength, CultureId);
            return new XBplusTree(tree);
        }
        public static new XBplusTree Initialize(System.IO.Stream treefile, System.IO.Stream blockfile, int KeyLength)
        {
            XBplusTreeBytes tree = XBplusTreeBytes.Initialize(treefile, blockfile, KeyLength);
            return new XBplusTree(tree);
        }

        public static new XBplusTree ReOpen(System.IO.Stream treefile, System.IO.Stream blockfile)
        {
            XBplusTreeBytes tree = XBplusTreeBytes.ReOpen(treefile, blockfile);
            return new XBplusTree(tree);
        }
        public static new XBplusTree ReOpen(string treefileName, string blockfileName)
        {
            XBplusTreeBytes tree = XBplusTreeBytes.ReOpen(treefileName, blockfileName);
            return new XBplusTree(tree);
        }
        public static new XBplusTree ReadOnly(string treefileName, string blockfileName)
        {
            XBplusTreeBytes tree = XBplusTreeBytes.ReadOnly(treefileName, blockfileName);
            return new XBplusTree(tree);
        }
    }
}
