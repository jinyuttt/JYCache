namespace BPlusTree
{
    /// <summary>
    /// Tree index mapping strings to strings with unlimited key length
    /// </summary>
    public class HBplusTree : BplusTree
    {
        HBplusTreeBytes xtree;
        public HBplusTree(HBplusTreeBytes tree)
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
        public static new HBplusTree Initialize(string treefileName, string blockfileName, int prefixLength, int cultureId,
            int nodesize, int buffersize)
        {
            HBplusTreeBytes tree = HBplusTreeBytes.Initialize(treefileName, blockfileName, prefixLength, cultureId, nodesize, buffersize);
            return new HBplusTree(tree);
        }
        public static new HBplusTree Initialize(string treefileName, string blockfileName, int prefixLength, int cultureId)
        {
            HBplusTreeBytes tree = HBplusTreeBytes.Initialize(treefileName, blockfileName, prefixLength, cultureId);
            return new HBplusTree(tree);
        }
        public static new HBplusTree Initialize(string treefileName, string blockfileName, int prefixLength)
        {
            HBplusTreeBytes tree = HBplusTreeBytes.Initialize(treefileName, blockfileName, prefixLength);
            return new HBplusTree(tree);
        }

        public static new HBplusTree Initialize(System.IO.Stream treefile, System.IO.Stream blockfile, int prefixLength, int cultureId,
            int nodesize, int buffersize)
        {
            HBplusTreeBytes tree = HBplusTreeBytes.Initialize(treefile, blockfile, prefixLength, cultureId, nodesize, buffersize);
            return new HBplusTree(tree);
        }
        public static new HBplusTree Initialize(System.IO.Stream treefile, System.IO.Stream blockfile, int prefixLength, int cultureId)
        {
            HBplusTreeBytes tree = HBplusTreeBytes.Initialize(treefile, blockfile, prefixLength, cultureId);
            return new HBplusTree(tree);
        }
        public static new HBplusTree Initialize(System.IO.Stream treefile, System.IO.Stream blockfile, int keyLength)
        {
            HBplusTreeBytes tree = HBplusTreeBytes.Initialize(treefile, blockfile, keyLength);
            return new HBplusTree(tree);
        }

        public static new HBplusTree ReOpen(System.IO.Stream treefile, System.IO.Stream blockfile)
        {
            HBplusTreeBytes tree = HBplusTreeBytes.ReOpen(treefile, blockfile);
            return new HBplusTree(tree);
        }
        public static new HBplusTree ReOpen(string treefileName, string blockfileName)
        {
            HBplusTreeBytes tree = HBplusTreeBytes.ReOpen(treefileName, blockfileName);
            return new HBplusTree(tree);
        }
        public static new HBplusTree ReadOnly(string treefileName, string blockfileName)
        {
            HBplusTreeBytes tree = HBplusTreeBytes.ReadOnly(treefileName, blockfileName);
            return new HBplusTree(tree);
        }
        public override string ToHtml()
        {
            return ((HBplusTreeBytes)this.tree).ToHtml();
        }
    }
}
