using System;
using System.Collections;

namespace BPlusTree
{
    /// <summary>
    /// Bplustree mapping fixed length strings (byte sequences) to longs (seek positions in file indexed).
    /// "Next leaf pointer" is not used since it increases the chance of file corruption on failure.
    /// All modifications are "shadowed" until a flush of all modifications succeeds.  Modifications are
    /// "hardened" when the header record is rewritten with a new root.  This design trades a few "unneeded"
    /// buffer writes for lower likelihood of file corruption.
    /// </summary>
    public class BplusTreeLong : ITreeIndex
    {
        public System.IO.Stream FromFileStream;
        public string FromFilePath;
        public bool DontUseCulture = false;
        public System.Globalization.CultureInfo CultureContext;
        System.Globalization.CompareInfo _cmp = null;
        public BufferFile Buffers;
        public int Buffersize;
        public int KeyLength;
        public long SeekStart = 0;
        public static byte[] HeaderPrefix = { 98, 112, 78, 98, 112 };
        // header consists of 
        // prefix | version | node size | key size | culture id | buffer number of root | buffer number of free list head
        int headersize = HeaderPrefix.Length + 1 + BufferFile.INTSTORAGE * 3 + BufferFile.LONGSTORAGE * 2;
        public const byte Version = 0;
        // size of allocated key space in each node (should be a read only property)
        public int NodeSize;
        BplusNode _root = null;
        long _rootSeek;
        long _freeHeadSeek;
        public Hashtable FreeBuffersOnCommit = new Hashtable();
        public Hashtable FreeBuffersOnAbort = new Hashtable();
        Hashtable IdToTerminalNode = new Hashtable();
        Hashtable TerminalNodeToId = new Hashtable();
        int _terminalNodeCount = 0;
        int _lowerTerminalNodeCount = 0;
        int _fifoLimit = 100;
        public static int NullBufferNumber = -1;
        public static byte NonLeaf = 0, Leaf = 1, Free = 2;

        public BplusTreeLong(System.IO.Stream fromFileStream, int keyLength, int nodeSize, int cultureId) :
            this(fromFileStream, nodeSize, keyLength, (long)0, cultureId)
        {
            // just start seek at 0
        }

        public BplusTreeLong(System.IO.Stream fromFileStream, int nodeSize, int keyLength, long startSeek, int cultureId) :
            this(fromFileStream, string.Empty, nodeSize, keyLength, (long)0, cultureId)
        {

        }

        public BplusTreeLong(System.IO.Stream fromFileStream, string fromFilePath, int nodeSize, int keyLength, long startSeek, int cultureId)
        {
            this.CultureContext = new System.Globalization.CultureInfo(cultureId);
            this._cmp = this.CultureContext.CompareInfo;
            this.FromFileStream = fromFileStream;
            this.FromFilePath = fromFilePath;
            this.NodeSize = nodeSize;
            this.SeekStart = startSeek;
            // add in key prefix overhead
            this.KeyLength = keyLength + BufferFile.SHORTSTORAGE;
            this._rootSeek = NullBufferNumber;
            this._root = null;
            this._freeHeadSeek = NullBufferNumber;
            this.SanityCheck();
        }

        public static BplusTreeLong SetupFromExistingStream(System.IO.Stream fromFile)
        {
            return SetupFromExistingStream(fromFile, (long)0);
        }
        public static BplusTreeLong SetupFromExistingStream(System.IO.Stream fromFile, string fromFilePath)
        {
            return SetupFromExistingStream(fromFile, fromFilePath, (long)0);
        }
        public static BplusTreeLong SetupFromExistingStream(System.IO.Stream fromFile, long startSeek)
        {
            return SetupFromExistingStream(fromFile, string.Empty, startSeek);
        }
        public static BplusTreeLong SetupFromExistingStream(System.IO.Stream fromFile, string fromFilePath, long startSeek)
        {
            int dummyId = System.Globalization.CultureInfo.InvariantCulture.LCID;
            BplusTreeLong result = new BplusTreeLong(fromFile, fromFilePath, 7, 100, startSeek, dummyId); // dummy values for nodesize, keysize
            result.ReadHeader();
            result.Buffers = BufferFile.SetupFromExistingStream(fromFile, result.Buffersize, startSeek + result.headersize);
            if (result.Buffers.buffersize != result.Buffersize)
            {
                throw new BplusTreeException("inner and outer buffer sizes should match");
            }
            if (result._rootSeek != NullBufferNumber)
            {
                result._root = new BplusNode(result, null, -1, true);
                result._root.LoadFromBuffer(result._rootSeek);
            }
            return result;
        }

        public static BplusTreeLong InitializeInStream(System.IO.Stream fromFile, int keyLength, int nodeSize)
        {
            int dummyId = System.Globalization.CultureInfo.InvariantCulture.LCID;
            return InitializeInStream(fromFile, keyLength, nodeSize, dummyId);
        }
        public static BplusTreeLong InitializeInStream(System.IO.Stream fromFile, int keyLength, int nodeSize, int cultureId)
        {
            return InitializeInStream(fromFile, keyLength, nodeSize, cultureId, (long)0);
        }
        public static BplusTreeLong InitializeInStream(System.IO.Stream fromFile, string fromFilePath, int keyLength, int nodeSize, int cultureId)
        {
            return InitializeInStream(fromFile, fromFilePath, keyLength, nodeSize, cultureId, (long)0);
        }
        public static BplusTreeLong InitializeInStream(System.IO.Stream fromFile, int keyLength, int nodeSize, int cultureId, long startSeek)
        {
            return InitializeInStream(fromFile, string.Empty, keyLength, nodeSize, cultureId, (long)0);
        }
        public static BplusTreeLong InitializeInStream(System.IO.Stream fromFile, string fromFilePath, int keyLength, int nodeSize, int cultureId, long startSeek)
        {
            if (fromFile.Length > startSeek)
            {
                throw new BplusTreeException("can't initialize bplus tree inside written area of stream");
            }
            BplusTreeLong result = new BplusTreeLong(fromFile, fromFilePath, nodeSize, keyLength, startSeek, cultureId);
            result.SetHeader();
            result.Buffers = BufferFile.InitializeBufferFileInStream(fromFile, result.Buffersize, startSeek + result.headersize);
            return result;
        }

        public int MaxKeyLength()
        {
            return this.KeyLength - BufferFile.SHORTSTORAGE;
        }
        public void Shutdown()
        {
            this.FromFileStream.Flush();
            this.FromFileStream.Close();
        }
        public int Compare(string left, string right)
        {
            //System.Globalization.CompareInfo cmp = this.cultureContext.CompareInfo;
            if (this.CultureContext == null || this.DontUseCulture)
            {
                // no culture context: use miscellaneous total ordering on unicode strings
                int i = 0;
                while (i < left.Length && i < right.Length)
                {
                    int leftOrd = Convert.ToInt32(left[i]);
                    int rightOrd = Convert.ToInt32(right[i]);
                    if (leftOrd < rightOrd)
                    {
                        return -1;
                    }
                    if (leftOrd > rightOrd)
                    {
                        return 1;
                    }
                    i++;
                }
                if (left.Length < right.Length)
                {
                    return -1;
                }
                if (left.Length > right.Length)
                {
                    return 1;
                }
                return 0;
            }
            if (this._cmp == null)
            {
                this._cmp = this.CultureContext.CompareInfo;
            }
            return this._cmp.Compare(left, right);
        }
        public void SanityCheck(bool strong)
        {
            this.SanityCheck();
            if (strong)
            {
                this.Recover(false);
                // look at all deferred deallocations -- they should not be free
                byte[] buffer = new byte[1];
                foreach (DictionaryEntry thing in this.FreeBuffersOnAbort)
                {
                    long buffernumber = (long)thing.Key;
                    this.Buffers.GetBuffer(buffernumber, buffer, 0, 1);
                    if (buffer[0] == Free)
                    {
                        throw new BplusTreeException("free on abort buffer already marked free " + buffernumber);
                    }
                }
                foreach (DictionaryEntry thing in this.FreeBuffersOnCommit)
                {
                    long buffernumber = (long)thing.Key;
                    this.Buffers.GetBuffer(buffernumber, buffer, 0, 1);
                    if (buffer[0] == Free)
                    {
                        throw new BplusTreeException("free on commit buffer already marked free " + buffernumber);
                    }
                }
            }
        }
        public void Recover(bool correctErrors)
        {
            Hashtable visited = new Hashtable();
            if (this._root != null)
            {
                // find all reachable nodes
                this._root.SanityCheck(visited);
            }
            // traverse the free list
            long freebuffernumber = this._freeHeadSeek;
            while (freebuffernumber != NullBufferNumber)
            {
                if (visited.ContainsKey(freebuffernumber))
                {
                    throw new BplusTreeException("free buffer visited twice " + freebuffernumber);
                }
                visited[freebuffernumber] = Free;
                freebuffernumber = this.ParseFreeBuffer(freebuffernumber);
            }
            // find out what is missing
            Hashtable missing = new Hashtable();
            long maxbuffer = this.Buffers.NextBufferNumber();
            for (long i = 0; i < maxbuffer; i++)
            {
                if (!visited.ContainsKey(i))
                {
                    missing[i] = i;
                }
            }
            // remove from missing any free-on-commit blocks
            foreach (DictionaryEntry thing in this.FreeBuffersOnCommit)
            {
                long tobefreed = (long)thing.Key;
                missing.Remove(tobefreed);
            }
            // add the missing values to the free list
            if (correctErrors)
            {
                if (missing.Count > 0)
                {
                    System.Diagnostics.Debug.WriteLine("correcting " + missing.Count + " unreachable buffers");
                }
                ArrayList missingL = new ArrayList();
                foreach (DictionaryEntry d in missing)
                {
                    missingL.Add(d.Key);
                }
                missingL.Sort();
                missingL.Reverse();
                foreach (object thing in missingL)
                {
                    long buffernumber = (long)thing;
                    this.DeallocateBuffer(buffernumber);
                }
                //this.ResetBookkeeping();
            }
            else if (missing.Count > 0)
            {
                string buffers = "";
                foreach (DictionaryEntry thing in missing)
                {
                    buffers += " " + thing.Key;
                }
                throw new BplusTreeException("found " + missing.Count + " unreachable buffers." + buffers);
            }
        }
        public void SerializationCheck()
        {
            if (this._root == null)
            {
                throw new BplusTreeException("serialization check requires initialized root, sorry");
            }
            this._root.SerializationCheck();
        }
        void SanityCheck()
        {
            if (this.NodeSize < 2)
            {
                throw new BplusTreeException("node size must be larger than 2");
            }
            if (this.KeyLength < 5)
            {
                throw new BplusTreeException("Key length must be larger than 5");
            }
            if (this.SeekStart < 0)
            {
                throw new BplusTreeException("start seek may not be negative");
            }
            // compute the buffer size
            // indicator | seek position | [ key storage | seek position ]*
            int keystorage = this.KeyLength + BufferFile.SHORTSTORAGE;
            this.Buffersize = 1 + BufferFile.LONGSTORAGE + (keystorage + BufferFile.LONGSTORAGE) * this.NodeSize;
        }
        public string ToHtml()
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            sb.Append("<h1>BplusTree</h1>\r\n");
            sb.Append("\r\n<br> nodesize=" + this.NodeSize);
            sb.Append("\r\n<br> seekstart=" + this.SeekStart);
            sb.Append("\r\n<br> rootseek=" + this._rootSeek);
            sb.Append("\r\n<br> free on commit " + this.FreeBuffersOnCommit.Count + " ::");
            foreach (DictionaryEntry thing in this.FreeBuffersOnCommit)
            {
                sb.Append(" " + thing.Key);
            }
            sb.Append("\r\n<br> Freebuffers : ");
            Hashtable freevisit = new Hashtable();
            long free = this._freeHeadSeek;
            string allfree = "freehead=" + free + " :: ";
            while (free != NullBufferNumber)
            {
                allfree = allfree + " " + free;
                if (freevisit.ContainsKey(free))
                {
                    throw new BplusTreeException("cycle in freelist " + free);
                }
                freevisit[free] = free;
                free = this.ParseFreeBuffer(free);
            }
            if (allfree.Length == 0)
            {
                sb.Append("empty list");
            }
            else
            {
                sb.Append(allfree);
            }
            foreach (DictionaryEntry thing in this.FreeBuffersOnCommit)
            {
                sb.Append(" " + thing.Key);
            }
            sb.Append("\r\n<br> free on abort " + this.FreeBuffersOnAbort.Count + " ::");
            foreach (DictionaryEntry thing in this.FreeBuffersOnAbort)
            {
                sb.Append(" " + thing.Key);
            }
            sb.Append("\r\n<br>\r\n");

            //... add more
            if (this._root == null)
            {
                sb.Append("<br><b>NULL ROOT</b>\r\n");
            }
            else
            {
                this._root.AsHtml(sb);
            }
            return sb.ToString();
        }

        public void SetFootPrintLimit(int limit)
        {
            if (limit < 5)
            {
                throw new BplusTreeException("foot print limit less than 5 is too small");
            }
            this._fifoLimit = limit;
        }
        public void RemoveKey(string key)
        {
            if (this._root == null)
            {
                throw new BplusTreeKeyMissing("tree is empty: cannot delete");
            }
            bool MergeMe;
            BplusNode theroot = this._root;
            theroot.Delete(key, out MergeMe);
            // if the root is not a leaf and contains only one child (no key), reroot
            if (MergeMe && !this._root.IsLeaf && this._root.SizeInUse() == 0)
            {
                this._root = this._root.FirstChild();
                this._rootSeek = this._root.MakeRoot();
                theroot.Free();
            }
        }
        public long this[string key]
        {
            get
            {
                long valueFound;
                bool test = this.ContainsKey(key, out valueFound);
                if (!test)
                {
                    throw new BplusTreeKeyMissing("no such key found: " + key);
                }
                return valueFound;
            }
            set
            {
                if (!BplusNode.KeyOK(key, this))
                {
                    throw new BplusTreeBadKeyValue("null or too large key cannot be inserted into tree: " + key);
                }
                bool rootinit = false;
                if (this._root == null)
                {
                    // allocate root
                    this._root = new BplusNode(this, null, -1, true);
                    rootinit = true;
                    //this.rootSeek = root.DumpToFreshBuffer();
                }
                // insert into root...
                string splitString;
                BplusNode splitNode;
                _root.Insert(key, value, out splitString, out splitNode);
                if (splitNode != null)
                {
                    // split of root: make a new root.
                    rootinit = true;
                    BplusNode oldRoot = this._root;
                    this._root = BplusNode.BinaryRoot(oldRoot, splitString, splitNode, this);
                }
                if (rootinit)
                {
                    this._rootSeek = _root.DumpToFreshBuffer();
                }
                // check size in memory
                this.ShrinkFootprint();
            }
        }
        public string FirstKey()
        {
            string result = null;
            if (this._root != null)
            {
                // empty string is smallest possible tree
                if (this.ContainsKey(""))
                {
                    result = "";
                }
                else
                {
                    return this._root.FindNextKey("");
                }
                this.ShrinkFootprint();
            }
            return result;
        }
        public string NextKey(string afterThisKey)
        {
            if (afterThisKey == null)
            {
                throw new BplusTreeBadKeyValue("cannot search for null string");
            }
            string result = this._root.FindNextKey(afterThisKey);
            this.ShrinkFootprint();
            return result;
        }
        public bool ContainsKey(string key)
        {
            long valueFound;
            return this.ContainsKey(key, out valueFound);
        }
        public bool ContainsKey(string key, out long valueFound)
        {
            if (key == null)
            {
                throw new BplusTreeBadKeyValue("cannot search for null string");
            }
            bool result = false;
            valueFound = (long)0;
            if (this._root != null)
            {
                result = this._root.FindMatch(key, out valueFound);
            }
            this.ShrinkFootprint();
            return result;
        }
        public long Get(string key, long defaultValue)
        {
            long result = defaultValue;
            long valueFound;
            if (this.ContainsKey(key, out valueFound))
            {
                result = valueFound;
            }
            return result;
        }
        public void Set(string key, object map)
        {
            if (!(map is long))
            {
                throw new BplusTreeBadKeyValue("only longs may be used as values in a BplusTreeLong: " + map);
            }
            this[key] = (long)map;
        }
        public object Get(string key, object defaultValue)
        {
            long valueFound;
            if (this.ContainsKey(key, out valueFound))
            {
                return (object)valueFound;
            }
            return defaultValue;
        }
        /// <summary>
        /// Store off any changed buffers, clear the fifo, free invalid buffers
        /// </summary>
        public void Commit()
        {
            // store all modifications
            if (this._root != null)
            {
                this._rootSeek = this._root.Invalidate(false);
            }
            this.FromFileStream.Flush();
            // commit the new root
            this.SetHeader();
            this.FromFileStream.Flush();
            // at this point the changes are committed, but some space is unreachable.
            // now free all unfreed buffers no longer in use
            ArrayList toFree = new ArrayList();
            foreach (DictionaryEntry d in this.FreeBuffersOnCommit)
            {
                toFree.Add(d.Key);
            }
            toFree.Sort();
            toFree.Reverse();
            foreach (object thing in toFree)
            {
                long buffernumber = (long)thing;
                this.DeallocateBuffer(buffernumber);
            }
            // store the free list head
            this.SetHeader();
            this.FromFileStream.Flush();
            this.ResetBookkeeping();
        }
        /// <summary>
        /// Forget all changes since last commit
        /// </summary>
        public void Abort()
        {
            // deallocate allocated blocks
            ArrayList toFree = new ArrayList();
            foreach (DictionaryEntry d in this.FreeBuffersOnAbort)
            {
                toFree.Add(d.Key);
            }
            toFree.Sort();
            toFree.Reverse();
            foreach (object thing in toFree)
            {
                long buffernumber = (long)thing;
                this.DeallocateBuffer(buffernumber);
            }
            long freehead = this._freeHeadSeek;
            // reread the header (except for freelist head)
            this.ReadHeader();
            // restore the root
            if (this._rootSeek == NullBufferNumber)
            {
                this._root = null; // nothing was committed
            }
            else
            {
                this._root.LoadFromBuffer(this._rootSeek);
            }
            this.ResetBookkeeping();
            this._freeHeadSeek = freehead;
            this.SetHeader(); // store new freelist head
            this.FromFileStream.Flush();
        }
        void ResetBookkeeping()
        {
            this.FreeBuffersOnCommit.Clear();
            this.FreeBuffersOnAbort.Clear();
            this.IdToTerminalNode.Clear();
            this.TerminalNodeToId.Clear();
        }
        public long AllocateBuffer()
        {
            long allocated = -1;
            if (this._freeHeadSeek == NullBufferNumber)
            {
                // should be written immediately after allocation
                allocated = this.Buffers.NextBufferNumber();
                //System.Diagnostics.Debug.WriteLine("<br> allocating fresh buffer "+allocated);
                return allocated;
            }
            // get the free head data
            allocated = this._freeHeadSeek;
            this._freeHeadSeek = this.ParseFreeBuffer(allocated);
            //System.Diagnostics.Debug.WriteLine("<br> recycling free buffer "+allocated);
            return allocated;
        }
        long ParseFreeBuffer(long buffernumber)
        {
            int freesize = 1 + BufferFile.LONGSTORAGE;
            byte[] buffer = new byte[freesize];
            this.Buffers.GetBuffer(buffernumber, buffer, 0, freesize);
            if (buffer[0] != Free)
            {
                throw new BplusTreeException("free buffer not marked free");
            }
            long result = BufferFile.RetrieveLong(buffer, 1);
            return result;
        }
        public void DeallocateBuffer(long buffernumber)
        {
            //System.Diagnostics.Debug.WriteLine("<br> deallocating "+buffernumber);
            int freesize = 1 + BufferFile.LONGSTORAGE;
            byte[] buffer = new byte[freesize];
            // it better not already be marked free
            this.Buffers.GetBuffer(buffernumber, buffer, 0, 1);
            if (buffer[0] == Free)
            {
                throw new BplusTreeException("attempt to re-free free buffer not allowed");
            }
            buffer[0] = Free;
            BufferFile.Store(this._freeHeadSeek, buffer, 1);
            this.Buffers.SetBuffer(buffernumber, buffer, 0, freesize);
            this._freeHeadSeek = buffernumber;
        }
        void SetHeader()
        {
            byte[] header = this.MakeHeader();
            this.FromFileStream.Seek(this.SeekStart, System.IO.SeekOrigin.Begin);
            this.FromFileStream.Write(header, 0, header.Length);
        }
        public void RecordTerminalNode(BplusNode terminalNode)
        {
            if (terminalNode == this._root)
            {
                return; // never record the root node
            }
            if (this.TerminalNodeToId.ContainsKey(terminalNode))
            {
                return; // don't record it again
            }
            int id = this._terminalNodeCount;
            this._terminalNodeCount++;
            this.TerminalNodeToId[terminalNode] = id;
            this.IdToTerminalNode[id] = terminalNode;
        }
        public void ForgetTerminalNode(BplusNode nonterminalNode)
        {
            if (!this.TerminalNodeToId.ContainsKey(nonterminalNode))
            {
                // silently ignore (?)
                return;
            }
            int id = (int)this.TerminalNodeToId[nonterminalNode];
            if (id == this._lowerTerminalNodeCount)
            {
                this._lowerTerminalNodeCount++;
            }
            this.IdToTerminalNode.Remove(id);
            this.TerminalNodeToId.Remove(nonterminalNode);
        }
        public void ShrinkFootprint()
        {
            this.InvalidateTerminalNodes(this._fifoLimit);
        }
        public void InvalidateTerminalNodes(int toLimit)
        {
            while (this.TerminalNodeToId.Count > toLimit)
            {
                // choose oldest nonterminal and deallocate it
                while (!this.IdToTerminalNode.ContainsKey(this._lowerTerminalNodeCount))
                {
                    this._lowerTerminalNodeCount++; // since most nodes are terminal this should usually be a short walk
                    //System.Diagnostics.Debug.WriteLine("<BR>WALKING "+this.LowerTerminalNodeCount);
                    //System.Console.WriteLine("<BR>WALKING "+this.LowerTerminalNodeCount);
                    if (this._lowerTerminalNodeCount > this._terminalNodeCount)
                    {
                        throw new BplusTreeException("internal error counting nodes, lower limit went too large");
                    }
                }
                //System.Console.WriteLine("<br> done walking");
                int id = this._lowerTerminalNodeCount;
                BplusNode victim = (BplusNode)this.IdToTerminalNode[id];
                //System.Diagnostics.Debug.WriteLine("\r\n<br>selecting "+victim.myBufferNumber+" for deallocation from fifo");
                this.IdToTerminalNode.Remove(id);
                this.TerminalNodeToId.Remove(victim);
                if (victim.MyBufferNumber != NullBufferNumber)
                {
                    victim.Invalidate(true);
                }
            }
        }
        void ReadHeader()
        {
            // prefix | version | node size | key size | culture id | buffer number of root | buffer number of free list head
            byte[] header = new byte[this.headersize];
            this.FromFileStream.Seek(this.SeekStart, System.IO.SeekOrigin.Begin);
            this.FromFileStream.Read(header, 0, this.headersize);
            int index = 0;
            // check prefix
            foreach (byte b in HeaderPrefix)
            {
                if (header[index] != b)
                {
                    throw new BufferFileException("invalid header prefix");
                }
                index++;
            }
            // skip version (for now)
            index++;
            this.NodeSize = BufferFile.Retrieve(header, index);
            index += BufferFile.INTSTORAGE;
            this.KeyLength = BufferFile.Retrieve(header, index);
            index += BufferFile.INTSTORAGE;
            int cultureId = BufferFile.Retrieve(header, index);
            this.CultureContext = new System.Globalization.CultureInfo(cultureId);
            index += BufferFile.INTSTORAGE;
            this._rootSeek = BufferFile.RetrieveLong(header, index);
            index += BufferFile.LONGSTORAGE;
            this._freeHeadSeek = BufferFile.RetrieveLong(header, index);
            this.SanityCheck();
            //this.header = header;
        }
        public byte[] MakeHeader()
        {
            // prefix | version | node size | key size | culture id | buffer number of root | buffer number of free list head
            byte[] result = new byte[this.headersize];
            HeaderPrefix.CopyTo(result, 0);
            result[HeaderPrefix.Length] = Version;
            int index = HeaderPrefix.Length + 1;
            BufferFile.Store(this.NodeSize, result, index);
            index += BufferFile.INTSTORAGE;
            BufferFile.Store(this.KeyLength, result, index);
            index += BufferFile.INTSTORAGE;
            if (this.CultureContext != null)
            {
                BufferFile.Store(this.CultureContext.LCID, result, index);
            }
            else
            {
                BufferFile.Store(System.Globalization.CultureInfo.InvariantCulture.LCID, result, index);
            }
            index += BufferFile.INTSTORAGE;
            BufferFile.Store(this._rootSeek, result, index);
            index += BufferFile.LONGSTORAGE;
            BufferFile.Store(this._freeHeadSeek, result, index);
            return result;
        }
    }
    public class BplusNode
    {
        public bool IsLeaf = true;
        // the maximum number of children to each node.
        int _size;
        // false if the node is no longer active and should not be used.
        //bool isValid = true;
        // true if the materialized node needs to be persisted.
        bool _dirty = true;
        // if non-root reference to the parent node containing this node
        BplusNode _parent = null;
        // tree containing this node
        BplusTreeLong _owner = null;
        // buffer number of this node
        public long MyBufferNumber = BplusTreeLong.NullBufferNumber;
        // number of children used by this node
        //int NumberOfValidKids = 0;
        long[] _childBufferNumbers;
        string[] _childKeys;
        BplusNode[] _materializedChildNodes;
        int _indexInParent = -1;

        /// <summary>
        /// Create a new BplusNode and install in parent if parent is not null.
        /// </summary>
        /// <param name="owner">tree containing the node</param>
        /// <param name="parent">parent node (if provided)</param>
        /// <param name="indexInParent">location in parent if provided</param>
        /// <param name="isLeaf"></param>
        public BplusNode(BplusTreeLong owner, BplusNode parent, int indexInParent, bool isLeaf)
        {
            this.IsLeaf = isLeaf;
            this._owner = owner;
            this._parent = parent;
            this._size = owner.NodeSize;
            //this.isValid = true;
            this._dirty = true;
            //			this.ChildBufferNumbers = new long[this.Size+1];
            //			this.ChildKeys = new string[this.Size];
            //			this.MaterializedChildNodes = new BplusNode[this.Size+1];
            this.Clear();
            if (parent != null && indexInParent >= 0)
            {
                if (indexInParent > this._size)
                {
                    throw new BplusTreeException("parent index too large");
                }
                // key info, etc, set elsewhere
                this._parent._materializedChildNodes[indexInParent] = this;
                this.MyBufferNumber = this._parent._childBufferNumbers[indexInParent];
                this._indexInParent = indexInParent;
            }
        }
        public BplusNode FirstChild()
        {
            BplusNode result = this.MaterializeNodeAtIndex(0);
            if (result == null)
            {
                throw new BplusTreeException("no first child");
            }
            return result;
        }
        public long MakeRoot()
        {
            this._parent = null;
            this._indexInParent = -1;
            if (this.MyBufferNumber == BplusTreeLong.NullBufferNumber)
            {
                throw new BplusTreeException("no root seek allocated to new root");
            }
            return this.MyBufferNumber;
        }
        public void Free()
        {
            if (this.MyBufferNumber != BplusTreeLong.NullBufferNumber)
            {
                if (this._owner.FreeBuffersOnAbort.ContainsKey(this.MyBufferNumber))
                {
                    // free it now
                    this._owner.FreeBuffersOnAbort.Remove(this.MyBufferNumber);
                    this._owner.DeallocateBuffer(this.MyBufferNumber);
                }
                else
                {
                    // free on commit
                    //this.owner.FreeBuffersOnCommit.Add(this.myBufferNumber);
                    this._owner.FreeBuffersOnCommit[this.MyBufferNumber] = this.MyBufferNumber;
                }
            }
            this.MyBufferNumber = BplusTreeLong.NullBufferNumber; // don't do it twice...
        }
        public void SerializationCheck()
        {
            BplusNode A = new BplusNode(this._owner, null, -1, false);
            for (int i = 0; i < this._size; i++)
            {
                long j = i * ((long)0xf0f0f0f0f0f0f01);
                A._childBufferNumbers[i] = j;
                A._childKeys[i] = "k" + i;
            }
            A._childBufferNumbers[this._size] = 7;
            A.TestRebuffer();
            A.IsLeaf = true;
            for (int i = 0; i < this._size; i++)
            {
                long j = -i * ((long)0x3e3e3e3e3e3e666);
                A._childBufferNumbers[i] = j;
                A._childKeys[i] = "key" + i;
            }
            A._childBufferNumbers[this._size] = -9097;
            A.TestRebuffer();
        }
        void TestRebuffer()
        {
            bool IL = this.IsLeaf;
            long[] Ns = this._childBufferNumbers;
            string[] Ks = this._childKeys;
            byte[] buffer = new byte[this._owner.Buffersize];
            this.Dump(buffer);
            this.Clear();
            this.Load(buffer);
            for (int i = 0; i < this._size; i++)
            {
                if (this._childBufferNumbers[i] != Ns[i])
                {
                    throw new BplusTreeException("didn't get back buffernumber " + i + " got " + this._childBufferNumbers[i] + " not " + Ns[i]);
                }
                if (!this._childKeys[i].Equals(Ks[i]))
                {
                    throw new BplusTreeException("didn't get back key " + i + " got " + this._childKeys[i] + " not " + Ks[i]);
                }
            }
            if (this._childBufferNumbers[this._size] != Ns[this._size])
            {
                throw new BplusTreeException("didn't get back buffernumber " + this._size + " got " + this._childBufferNumbers[this._size] + " not " + Ns[this._size]);
            }
            if (this.IsLeaf != IL)
            {
                throw new BplusTreeException("isLeaf should be " + IL + " got " + this.IsLeaf);
            }
        }
        public string SanityCheck(Hashtable visited)
        {
            string result = null;
            if (visited == null)
            {
                visited = new Hashtable();
            }
            if (visited.ContainsKey(this))
            {
                throw new BplusTreeException("node visited twice " + this.MyBufferNumber);
            }
            visited[this] = this.MyBufferNumber;
            if (this.MyBufferNumber != BplusTreeLong.NullBufferNumber)
            {
                if (visited.ContainsKey(this.MyBufferNumber))
                {
                    throw new BplusTreeException("buffer number seen twice " + this.MyBufferNumber);
                }
                visited[this.MyBufferNumber] = this;
            }
            if (this._parent != null)
            {
                if (this._parent.IsLeaf)
                {
                    throw new BplusTreeException("parent is leaf");
                }
                this._parent.MaterializeNodeAtIndex(this._indexInParent);
                if (this._parent._materializedChildNodes[this._indexInParent] != this)
                {
                    throw new BplusTreeException("incorrect index in parent");
                }
                // since not at root there should be at least size/2 keys
                int limit = this._size / 2;
                if (this.IsLeaf)
                {
                    limit--;
                }
                for (int i = 0; i < limit; i++)
                {
                    if (this._childKeys[i] == null)
                    {
                        throw new BplusTreeException("null child in first half");
                    }
                }
            }
            result = this._childKeys[0]; // for leaf
            if (!this.IsLeaf)
            {
                this.MaterializeNodeAtIndex(0);
                result = this._materializedChildNodes[0].SanityCheck(visited);
                for (int i = 0; i < this._size; i++)
                {
                    if (this._childKeys[i] == null)
                    {
                        break;
                    }
                    this.MaterializeNodeAtIndex(i + 1);
                    string least = this._materializedChildNodes[i + 1].SanityCheck(visited);
                    if (least == null)
                    {
                        throw new BplusTreeException("null least in child doesn't match node entry " + this._childKeys[i]);
                    }
                    if (!least.Equals(this._childKeys[i]))
                    {
                        throw new BplusTreeException("least in child " + least + " doesn't match node entry " + this._childKeys[i]);
                    }
                }
            }
            // look for duplicate keys
            string lastkey = this._childKeys[0];
            for (int i = 1; i < this._size; i++)
            {
                if (this._childKeys[i] == null)
                {
                    break;
                }
                if (lastkey.Equals(this._childKeys[i]))
                {
                    throw new BplusTreeException("duplicate key in node " + lastkey);
                }
                lastkey = this._childKeys[i];
            }
            return result;
        }
        void Destroy()
        {
            // make sure the structure is useless, it should no longer be used.
            this._owner = null;
            this._parent = null;
            this._size = -100;
            this._childBufferNumbers = null;
            this._childKeys = null;
            this._materializedChildNodes = null;
            this.MyBufferNumber = BplusTreeLong.NullBufferNumber;
            this._indexInParent = -100;
            this._dirty = false;
        }
        public int SizeInUse()
        {
            int result = 0;
            for (int i = 0; i < this._size; i++)
            {
                if (this._childKeys[i] == null)
                {
                    break;
                }
                result++;
            }
            return result;
        }
        public static BplusNode BinaryRoot(BplusNode LeftNode, string key, BplusNode RightNode, BplusTreeLong owner)
        {
            BplusNode newRoot = new BplusNode(owner, null, -1, false);
            //newRoot.Clear(); // redundant
            newRoot._childKeys[0] = key;
            LeftNode.Reparent(newRoot, 0);
            RightNode.Reparent(newRoot, 1);
            // new root is stored elsewhere
            return newRoot;
        }
        void Reparent(BplusNode newParent, int ParentIndex)
        {
            // keys and existing parent structure must be updated elsewhere.
            this._parent = newParent;
            this._indexInParent = ParentIndex;
            newParent._childBufferNumbers[ParentIndex] = this.MyBufferNumber;
            newParent._materializedChildNodes[ParentIndex] = this;
            // parent is no longer terminal
            this._owner.ForgetTerminalNode(_parent);
        }
        void Clear()
        {
            this._childBufferNumbers = new long[this._size + 1];
            this._childKeys = new string[this._size];
            this._materializedChildNodes = new BplusNode[this._size + 1];
            for (int i = 0; i < this._size; i++)
            {
                this._childBufferNumbers[i] = BplusTreeLong.NullBufferNumber;
                this._materializedChildNodes[i] = null;
                this._childKeys[i] = null;
            }
            this._childBufferNumbers[this._size] = BplusTreeLong.NullBufferNumber;
            this._materializedChildNodes[this._size] = null;
            // this is now a terminal node
            this._owner.RecordTerminalNode(this);
        }
        /// <summary>
        /// Find first index in self associated with a key same or greater than CompareKey
        /// </summary>
        /// <param name="compareKey">CompareKey</param>
        /// <param name="lookPastOnly">if true and this is a leaf then look for a greater value</param>
        /// <returns>lowest index of same or greater key or this.Size if no greater key.</returns>
        int FindAtOrNextPosition(string compareKey, bool lookPastOnly)
        {
            int insertposition = 0;
            //System.Globalization.CultureInfo culture = this.owner.cultureContext;
            //System.Globalization.CompareInfo cmp = culture.CompareInfo;
            if (this.IsLeaf && !lookPastOnly)
            {
                // look for exact match or greater or null
                while (insertposition < this._size && this._childKeys[insertposition] != null &&
                    //cmp.Compare(this.ChildKeys[insertposition], CompareKey)<0) 
                    this._owner.Compare(this._childKeys[insertposition], compareKey) < 0)
                {
                    insertposition++;
                }
            }
            else
            {
                // look for greater or null only
                while (insertposition < this._size && this._childKeys[insertposition] != null &&
                    this._owner.Compare(this._childKeys[insertposition], compareKey) <= 0)
                {
                    insertposition++;
                }
            }
            return insertposition;
        }
        /// <summary>
        /// Find the first key below atIndex, or if no such node traverse to the next key to the right.
        /// If no such key exists, return nulls.
        /// </summary>
        /// <param name="atIndex">where to look in this node</param>
        /// <param name="foundInLeaf">leaf where found</param>
        /// <param name="keyFound">key value found</param>
        void TraverseToFollowingKey(int atIndex, out BplusNode foundInLeaf, out string keyFound)
        {
            foundInLeaf = null;
            keyFound = null;
            bool lookInParent = false;
            if (this.IsLeaf)
            {
                lookInParent = (atIndex >= this._size) || (this._childKeys[atIndex] == null);
            }
            else
            {
                lookInParent = (atIndex > this._size) ||
                    (atIndex > 0 && this._childKeys[atIndex - 1] == null);
            }
            if (lookInParent)
            {
                // if it's anywhere it's in the next child of parent
                if (this._parent != null && this._indexInParent >= 0)
                {
                    this._parent.TraverseToFollowingKey(this._indexInParent + 1, out foundInLeaf, out keyFound);
                    return;
                }
                else
                {
                    return; // no such following key
                }
            }
            if (this.IsLeaf)
            {
                // leaf, we found it.
                foundInLeaf = this;
                keyFound = this._childKeys[atIndex];
                return;
            }
            else
            {
                // nonleaf, look in child (if there is one)
                if (atIndex == 0 || this._childKeys[atIndex - 1] != null)
                {
                    BplusNode thechild = this.MaterializeNodeAtIndex(atIndex);
                    thechild.TraverseToFollowingKey(0, out foundInLeaf, out keyFound);
                }
            }
        }
        public bool FindMatch(string compareKey, out long valueFound)
        {
            valueFound = 0; // dummy value on failure
            BplusNode leaf;
            int position = this.FindAtOrNextPositionInLeaf(compareKey, out leaf, false);
            if (position < leaf._size)
            {
                string key = leaf._childKeys[position];
                if ((key != null) && this._owner.Compare(key, compareKey) == 0) //(key.Equals(CompareKey)
                {
                    valueFound = leaf._childBufferNumbers[position];
                    return true;
                }
            }
            return false;
        }
        public string FindNextKey(string compareKey)
        {
            string result = null;
            BplusNode leaf;
            int position = this.FindAtOrNextPositionInLeaf(compareKey, out leaf, true);
            if (position >= leaf._size || leaf._childKeys[position] == null)
            {
                // try to traverse to the right.
                BplusNode newleaf;
                leaf.TraverseToFollowingKey(leaf._size, out newleaf, out result);
            }
            else
            {
                result = leaf._childKeys[position];
            }
            return result;
        }
        /// <summary>
        /// Find near-index of comparekey in leaf under this node. 
        /// </summary>
        /// <param name="compareKey">the key to look for</param>
        /// <param name="inLeaf">the leaf where found</param>
        /// <param name="lookPastOnly">If true then only look for a greater value, not an exact match.</param>
        /// <returns>index of match in leaf</returns>
        int FindAtOrNextPositionInLeaf(string compareKey, out BplusNode inLeaf, bool lookPastOnly)
        {
            int myposition = this.FindAtOrNextPosition(compareKey, lookPastOnly);
            if (this.IsLeaf)
            {
                inLeaf = this;
                return myposition;
            }
            long childBufferNumber = this._childBufferNumbers[myposition];
            if (childBufferNumber == BplusTreeLong.NullBufferNumber)
            {
                throw new BplusTreeException("can't search null subtree");
            }
            BplusNode child = this.MaterializeNodeAtIndex(myposition);
            return child.FindAtOrNextPositionInLeaf(compareKey, out inLeaf, lookPastOnly);
        }
        BplusNode MaterializeNodeAtIndex(int myposition)
        {
            if (this.IsLeaf)
            {
                throw new BplusTreeException("cannot materialize child for leaf");
            }
            long childBufferNumber = this._childBufferNumbers[myposition];
            if (childBufferNumber == BplusTreeLong.NullBufferNumber)
            {
                throw new BplusTreeException("can't search null subtree at position " + myposition + " in " + this.MyBufferNumber);
            }
            // is it already materialized?
            BplusNode result = this._materializedChildNodes[myposition];
            if (result != null)
            {
                return result;
            }
            // otherwise read it in...
            result = new BplusNode(this._owner, this, myposition, true); // dummy isLeaf value
            result.LoadFromBuffer(childBufferNumber);
            this._materializedChildNodes[myposition] = result;
            // no longer terminal
            this._owner.ForgetTerminalNode(this);
            return result;
        }
        public void LoadFromBuffer(long bufferNumber)
        {
            // freelist bookkeeping done elsewhere
            string parentinfo = "no parent"; // debug
            if (this._parent != null)
            {
                parentinfo = "parent=" + _parent.MyBufferNumber; // debug
            }
            //System.Diagnostics.Debug.WriteLine("\r\n<br> loading "+this.indexInParent+" from "+bufferNumber+" for "+parentinfo);
            byte[] rawdata = new byte[this._owner.Buffersize];
            this._owner.Buffers.GetBuffer(bufferNumber, rawdata, 0, rawdata.Length);
            this.Load(rawdata);
            this._dirty = false;
            this.MyBufferNumber = bufferNumber;
            // it's terminal until a child is materialized
            this._owner.RecordTerminalNode(this);
        }
        public long DumpToFreshBuffer()
        {
            long oldbuffernumber = this.MyBufferNumber;
            long freshBufferNumber = this._owner.AllocateBuffer();
            //System.Diagnostics.Debug.WriteLine("\r\n<br> dumping "+this.indexInParent+" from "+oldbuffernumber+" to "+freshBufferNumber);
            this.DumpToBuffer(freshBufferNumber);
            if (oldbuffernumber != BplusTreeLong.NullBufferNumber)
            {
                //this.owner.FreeBuffersOnCommit.Add(oldbuffernumber);
                if (this._owner.FreeBuffersOnAbort.ContainsKey(oldbuffernumber))
                {
                    // free it now
                    this._owner.FreeBuffersOnAbort.Remove(oldbuffernumber);
                    this._owner.DeallocateBuffer(oldbuffernumber);
                }
                else
                {
                    // free on commit
                    this._owner.FreeBuffersOnCommit[oldbuffernumber] = oldbuffernumber;
                }
            }
            //this.owner.FreeBuffersOnAbort.Add(freshBufferNumber);
            this._owner.FreeBuffersOnAbort[freshBufferNumber] = freshBufferNumber;
            return freshBufferNumber;
        }
        void DumpToBuffer(long buffernumber)
        {
            byte[] rawdata = new byte[this._owner.Buffersize];
            this.Dump(rawdata);
            this._owner.Buffers.SetBuffer(buffernumber, rawdata, 0, rawdata.Length);
            this._dirty = false;
            this.MyBufferNumber = buffernumber;
            if (this._parent != null && this._indexInParent >= 0 &&
                this._parent._childBufferNumbers[this._indexInParent] != buffernumber)
            {
                if (this._parent._materializedChildNodes[this._indexInParent] != this)
                {
                    throw new BplusTreeException("invalid parent connection " + this._parent.MyBufferNumber + " at " + this._indexInParent);
                }
                this._parent._childBufferNumbers[this._indexInParent] = buffernumber;
                this._parent.Soil();
            }
        }
        void ReParentAllChildren()
        {
            for (int i = 0; i <= this._size; i++)
            {
                BplusNode thisnode = this._materializedChildNodes[i];
                if (thisnode != null)
                {
                    thisnode.Reparent(this, i);
                }
            }
        }
        /// <summary>
        /// Delete entry for key
        /// </summary>
        /// <param name="key">key to delete</param>
        /// <param name="mergeMe">true if the node is less than half full after deletion</param>
        /// <returns>null unless the smallest key under this node has changed in which case it returns the smallest key.</returns>
        public string Delete(string key, out bool mergeMe)
        {
            mergeMe = false; // assumption
            string result = null;
            if (this.IsLeaf)
            {
                return this.DeleteLeaf(key, out mergeMe);
            }
            int deleteposition = this.FindAtOrNextPosition(key, false);
            long deleteBufferNumber = this._childBufferNumbers[deleteposition];
            if (deleteBufferNumber == BplusTreeLong.NullBufferNumber)
            {
                throw new BplusTreeException("key not followed by buffer number in non-leaf (del)");
            }
            // del in subtree
            BplusNode deleteChild = this.MaterializeNodeAtIndex(deleteposition);
            bool mergeKid;
            string delresult = deleteChild.Delete(key, out mergeKid);
            // delete succeeded... now fix up the child node if needed.
            this.Soil(); // redundant ?
            // bizarre special case for 2-3  or 3-4 trees -- empty leaf
            if (delresult != null && this._owner.Compare(delresult, key) == 0) // delresult.Equals(key)
            {
                if (this._size > 3)
                {
                    throw new BplusTreeException("assertion error: delete returned delete key for too large node size: " + this._size);
                }
                // junk this leaf and shift everything over
                if (deleteposition == 0)
                {
                    result = this._childKeys[deleteposition];
                }
                else if (deleteposition == this._size)
                {
                    this._childKeys[deleteposition - 1] = null;
                }
                else
                {
                    this._childKeys[deleteposition - 1] = this._childKeys[deleteposition];
                }
                if (result != null && this._owner.Compare(result, key) == 0) // result.Equals(key)
                {
                    // I'm not sure this ever happens
                    this.MaterializeNodeAtIndex(1);
                    result = this._materializedChildNodes[1].LeastKey();
                }
                deleteChild.Free();
                for (int i = deleteposition; i < this._size - 1; i++)
                {
                    this._childKeys[i] = this._childKeys[i + 1];
                    this._materializedChildNodes[i] = this._materializedChildNodes[i + 1];
                    this._childBufferNumbers[i] = this._childBufferNumbers[i + 1];
                }
                this._childKeys[this._size - 1] = null;
                if (deleteposition < this._size)
                {
                    this._materializedChildNodes[this._size - 1] = this._materializedChildNodes[this._size];
                    this._childBufferNumbers[this._size - 1] = this._childBufferNumbers[this._size];
                }
                this._materializedChildNodes[this._size] = null;
                this._childBufferNumbers[this._size] = BplusTreeLong.NullBufferNumber;
                mergeMe = (this.SizeInUse() < this._size / 2);
                this.ReParentAllChildren();
                return result;
            }
            if (deleteposition == 0)
            {
                // smallest key may have changed.
                result = delresult;
            }
            // update key array if needed
            else if (delresult != null && deleteposition > 0)
            {
                if (this._owner.Compare(delresult, key) != 0) // !delresult.Equals(key)
                {
                    this._childKeys[deleteposition - 1] = delresult;
                }
            }
            // if the child needs merging... do it
            if (mergeKid)
            {
                int leftindex, rightindex;
                BplusNode leftNode;
                BplusNode rightNode;
                string keyBetween;
                if (deleteposition == 0)
                {
                    // merge with next
                    leftindex = deleteposition;
                    rightindex = deleteposition + 1;
                    leftNode = deleteChild;
                    //keyBetween = this.ChildKeys[deleteposition];
                    rightNode = this.MaterializeNodeAtIndex(rightindex);
                }
                else
                {
                    // merge with previous
                    leftindex = deleteposition - 1;
                    rightindex = deleteposition;
                    leftNode = this.MaterializeNodeAtIndex(leftindex);
                    //keyBetween = this.ChildKeys[deleteBufferNumber-1];
                    rightNode = deleteChild;
                }
                keyBetween = this._childKeys[leftindex];
                string rightLeastKey;
                bool deleteRight;
                Merge(leftNode, keyBetween, rightNode, out rightLeastKey, out deleteRight);
                // delete the right node if needed.
                if (deleteRight)
                {
                    for (int i = rightindex; i < this._size; i++)
                    {
                        this._childKeys[i - 1] = this._childKeys[i];
                        this._childBufferNumbers[i] = this._childBufferNumbers[i + 1];
                        this._materializedChildNodes[i] = this._materializedChildNodes[i + 1];
                    }
                    this._childKeys[this._size - 1] = null;
                    this._materializedChildNodes[this._size] = null;
                    this._childBufferNumbers[this._size] = BplusTreeLong.NullBufferNumber;
                    this.ReParentAllChildren();
                    rightNode.Free();
                    // does this node need merging?
                    if (this.SizeInUse() < this._size / 2)
                    {
                        mergeMe = true;
                    }
                }
                else
                {
                    // update the key entry
                    this._childKeys[rightindex - 1] = rightLeastKey;
                }
            }
            return result;
        }
        string LeastKey()
        {
            string result = null;
            if (this.IsLeaf)
            {
                result = this._childKeys[0];
            }
            else
            {
                this.MaterializeNodeAtIndex(0);
                result = this._materializedChildNodes[0].LeastKey();
            }
            if (result == null)
            {
                throw new BplusTreeException("no key found");
            }
            return result;
        }
        public static void Merge(BplusNode left, string keyBetween, BplusNode right, out string rightLeastKey,
            out bool deleteRight)
        {
            //System.Diagnostics.Debug.WriteLine("\r\n<br> merging "+right.myBufferNumber+" ("+KeyBetween+") "+left.myBufferNumber);
            //System.Diagnostics.Debug.WriteLine(left.owner.toHtml());
            rightLeastKey = null; // only if DeleteRight
            if (left.IsLeaf || right.IsLeaf)
            {
                if (!(left.IsLeaf && right.IsLeaf))
                {
                    throw new BplusTreeException("can't merge leaf with non-leaf");
                }
                MergeLeaves(left, right, out deleteRight);
                rightLeastKey = right._childKeys[0];
                return;
            }
            // merge non-leaves
            deleteRight = false;
            string[] allkeys = new string[left._size * 2 + 1];
            long[] allseeks = new long[left._size * 2 + 2];
            BplusNode[] allMaterialized = new BplusNode[left._size * 2 + 2];
            if (left._childBufferNumbers[0] == BplusTreeLong.NullBufferNumber ||
                right._childBufferNumbers[0] == BplusTreeLong.NullBufferNumber)
            {
                throw new BplusTreeException("cannot merge empty non-leaf with non-leaf");
            }
            int index = 0;
            allseeks[0] = left._childBufferNumbers[0];
            allMaterialized[0] = left._materializedChildNodes[0];
            for (int i = 0; i < left._size; i++)
            {
                if (left._childKeys[i] == null)
                {
                    break;
                }
                allkeys[index] = left._childKeys[i];
                allseeks[index + 1] = left._childBufferNumbers[i + 1];
                allMaterialized[index + 1] = left._materializedChildNodes[i + 1];
                index++;
            }
            allkeys[index] = keyBetween;
            index++;
            allseeks[index] = right._childBufferNumbers[0];
            allMaterialized[index] = right._materializedChildNodes[0];
            int rightcount = 0;
            for (int i = 0; i < right._size; i++)
            {
                if (right._childKeys[i] == null)
                {
                    break;
                }
                allkeys[index] = right._childKeys[i];
                allseeks[index + 1] = right._childBufferNumbers[i + 1];
                allMaterialized[index + 1] = right._materializedChildNodes[i + 1];
                index++;
                rightcount++;
            }
            if (index <= left._size)
            {
                // it will all fit in one node
                //System.Diagnostics.Debug.WriteLine("deciding to forget "+right.myBufferNumber+" into "+left.myBufferNumber);
                deleteRight = true;
                for (int i = 0; i < index; i++)
                {
                    left._childKeys[i] = allkeys[i];
                    left._childBufferNumbers[i] = allseeks[i];
                    left._materializedChildNodes[i] = allMaterialized[i];
                }
                left._childBufferNumbers[index] = allseeks[index];
                left._materializedChildNodes[index] = allMaterialized[index];
                left.ReParentAllChildren();
                left.Soil();
                right.Free();
                return;
            }
            // otherwise split the content between the nodes
            left.Clear();
            right.Clear();
            left.Soil();
            right.Soil();
            int leftcontent = index / 2;
            int rightcontent = index - leftcontent - 1;
            rightLeastKey = allkeys[leftcontent];
            int outputindex = 0;
            for (int i = 0; i < leftcontent; i++)
            {
                left._childKeys[i] = allkeys[outputindex];
                left._childBufferNumbers[i] = allseeks[outputindex];
                left._materializedChildNodes[i] = allMaterialized[outputindex];
                outputindex++;
            }
            rightLeastKey = allkeys[outputindex];
            left._childBufferNumbers[outputindex] = allseeks[outputindex];
            left._materializedChildNodes[outputindex] = allMaterialized[outputindex];
            outputindex++;
            rightcount = 0;
            for (int i = 0; i < rightcontent; i++)
            {
                right._childKeys[i] = allkeys[outputindex];
                right._childBufferNumbers[i] = allseeks[outputindex];
                right._materializedChildNodes[i] = allMaterialized[outputindex];
                outputindex++;
                rightcount++;
            }
            right._childBufferNumbers[rightcount] = allseeks[outputindex];
            right._materializedChildNodes[rightcount] = allMaterialized[outputindex];
            left.ReParentAllChildren();
            right.ReParentAllChildren();
        }
        public static void MergeLeaves(BplusNode left, BplusNode right, out bool deleteRight)
        {
            deleteRight = false;
            string[] allkeys = new string[left._size * 2];
            long[] allseeks = new long[left._size * 2];
            int index = 0;
            for (int i = 0; i < left._size; i++)
            {
                if (left._childKeys[i] == null)
                {
                    break;
                }
                allkeys[index] = left._childKeys[i];
                allseeks[index] = left._childBufferNumbers[i];
                index++;
            }
            for (int i = 0; i < right._size; i++)
            {
                if (right._childKeys[i] == null)
                {
                    break;
                }
                allkeys[index] = right._childKeys[i];
                allseeks[index] = right._childBufferNumbers[i];
                index++;
            }
            if (index <= left._size)
            {
                left.Clear();
                deleteRight = true;
                for (int i = 0; i < index; i++)
                {
                    left._childKeys[i] = allkeys[i];
                    left._childBufferNumbers[i] = allseeks[i];
                }
                right.Free();
                left.Soil();
                return;
            }
            left.Clear();
            right.Clear();
            left.Soil();
            right.Soil();
            int rightcontent = index / 2;
            int leftcontent = index - rightcontent;
            int newindex = 0;
            for (int i = 0; i < leftcontent; i++)
            {
                left._childKeys[i] = allkeys[newindex];
                left._childBufferNumbers[i] = allseeks[newindex];
                newindex++;
            }
            for (int i = 0; i < rightcontent; i++)
            {
                right._childKeys[i] = allkeys[newindex];
                right._childBufferNumbers[i] = allseeks[newindex];
                newindex++;
            }
        }
        public string DeleteLeaf(string key, out bool mergeMe)
        {
            string result = null;
            mergeMe = false;
            bool found = false;
            int deletelocation = 0;
            foreach (string thiskey in this._childKeys)
            {
                // use comparison, not equals, in case different strings sometimes compare same
                if (thiskey != null && this._owner.Compare(thiskey, key) == 0) // thiskey.Equals(key)
                {
                    found = true;
                    break;
                }
                deletelocation++;
            }
            if (!found)
            {
                throw new BplusTreeKeyMissing("cannot delete missing key: " + key);
            }
            this.Soil();
            // only keys are important...
            for (int i = deletelocation; i < this._size - 1; i++)
            {
                this._childKeys[i] = this._childKeys[i + 1];
                this._childBufferNumbers[i] = this._childBufferNumbers[i + 1];
            }
            this._childKeys[this._size - 1] = null;
            //this.MaterializedChildNodes[endlocation+1] = null;
            //this.ChildBufferNumbers[endlocation+1] = BplusTreeLong.NullBufferNumber;
            if (this.SizeInUse() < this._size / 2)
            {
                mergeMe = true;
            }
            if (deletelocation == 0)
            {
                result = this._childKeys[0];
                // this is only relevant for the case of 2-3 trees (empty leaf after deletion)
                if (result == null)
                {
                    result = key; // deleted value
                }
            }
            return result;
        }
        /// <summary>
        /// insert key/position entry in self 
        /// </summary>
        /// <param name="key">Key to associate with the leaf</param>
        /// <param name="position">position associated with key in external structur</param>
        /// <param name="splitString">if not null then the smallest key in the new split leaf</param>
        /// <param name="splitNode">if not null then the node was split and this is the leaf to the right.</param>
        /// <returns>null unless the smallest key under this node has changed, in which case it returns the smallest key.</returns>
        public string Insert(string key, long position, out string splitString, out BplusNode splitNode)
        {
            if (this.IsLeaf)
            {
                return this.InsertLeaf(key, position, out splitString, out splitNode);
            }
            splitString = null;
            splitNode = null;
            int insertposition = this.FindAtOrNextPosition(key, false);
            long insertBufferNumber = this._childBufferNumbers[insertposition];
            if (insertBufferNumber == BplusTreeLong.NullBufferNumber)
            {
                throw new BplusTreeException("key not followed by buffer number in non-leaf");
            }
            // insert in subtree
            BplusNode insertChild = this.MaterializeNodeAtIndex(insertposition);
            BplusNode childSplit;
            string childSplitString;
            string childInsert = insertChild.Insert(key, position, out childSplitString, out childSplit);
            // if there was a split the node must expand
            if (childSplit != null)
            {
                // insert the child
                this.Soil(); // redundant -- a child will have a change so this node will need to be copied
                int newChildPosition = insertposition + 1;
                bool dosplit = false;
                // if there is no free space we must do a split
                if (this._childBufferNumbers[this._size] != BplusTreeLong.NullBufferNumber)
                {
                    dosplit = true;
                    this.PrepareForSplit();
                }
                // bubble over the current values to make space for new child
                for (int i = this._childKeys.Length - 2; i >= newChildPosition - 1; i--)
                {
                    int i1 = i + 1;
                    int i2 = i1 + 1;
                    this._childKeys[i1] = this._childKeys[i];
                    this._childBufferNumbers[i2] = this._childBufferNumbers[i1];
                    BplusNode childNode = this._materializedChildNodes[i2] = this._materializedChildNodes[i1];
                }
                // record the new child
                this._childKeys[newChildPosition - 1] = childSplitString;
                //this.MaterializedChildNodes[newChildPosition] = childSplit;
                //this.ChildBufferNumbers[newChildPosition] = childSplit.myBufferNumber;
                childSplit.Reparent(this, newChildPosition);
                // split, if needed
                if (dosplit)
                {
                    int splitpoint = this._materializedChildNodes.Length / 2 - 1;
                    splitString = this._childKeys[splitpoint];
                    splitNode = new BplusNode(this._owner, this._parent, -1, this.IsLeaf);
                    // make copy of expanded node structure
                    BplusNode[] materialized = this._materializedChildNodes;
                    long[] buffernumbers = this._childBufferNumbers;
                    string[] keys = this._childKeys;
                    // repair the expanded node
                    this._childKeys = new string[this._size];
                    this._materializedChildNodes = new BplusNode[this._size + 1];
                    this._childBufferNumbers = new long[this._size + 1];
                    this.Clear();
                    Array.Copy(materialized, 0, this._materializedChildNodes, 0, splitpoint + 1);
                    Array.Copy(buffernumbers, 0, this._childBufferNumbers, 0, splitpoint + 1);
                    Array.Copy(keys, 0, this._childKeys, 0, splitpoint);
                    // initialize the new node
                    splitNode.Clear(); // redundant.
                    int remainingKeys = this._size - splitpoint;
                    Array.Copy(materialized, splitpoint + 1, splitNode._materializedChildNodes, 0, remainingKeys + 1);
                    Array.Copy(buffernumbers, splitpoint + 1, splitNode._childBufferNumbers, 0, remainingKeys + 1);
                    Array.Copy(keys, splitpoint + 1, splitNode._childKeys, 0, remainingKeys);
                    // fix pointers in materialized children of splitnode
                    splitNode.ReParentAllChildren();
                    // store the new node
                    splitNode.DumpToFreshBuffer();
                    splitNode.CheckIfTerminal();
                    splitNode.Soil();
                    this.CheckIfTerminal();
                }
                // fix pointers in children
                this.ReParentAllChildren();
            }
            if (insertposition == 0)
            {
                // the smallest key may have changed
                return childInsert;
            }
            return null;  // no change in smallest key
        }
        /// <summary>
        /// Check to see if this is a terminal node, if so record it, otherwise forget it
        /// </summary>
        void CheckIfTerminal()
        {
            if (!this.IsLeaf)
            {
                for (int i = 0; i < this._size + 1; i++)
                {
                    if (this._materializedChildNodes[i] != null)
                    {
                        this._owner.ForgetTerminalNode(this);
                        return;
                    }
                }
            }
            this._owner.RecordTerminalNode(this);
        }
        /// <summary>
        /// insert key/position entry in self (as leaf)
        /// </summary>
        /// <param name="key">Key to associate with the leaf</param>
        /// <param name="position">position associated with key in external structure</param>
        /// <param name="splitString">if not null then the smallest key in the new split leaf</param>
        /// <param name="splitNode">if not null then the node was split and this is the leaf to the right.</param>
        /// <returns>smallest key value in keys, or null if no change</returns>
        public string InsertLeaf(string key, long position, out string splitString, out BplusNode splitNode)
        {
            splitString = null;
            splitNode = null;
            bool dosplit = false;
            if (!this.IsLeaf)
            {
                throw new BplusTreeException("bad call to InsertLeaf: this is not a leaf");
            }
            this.Soil();
            int insertposition = this.FindAtOrNextPosition(key, false);
            if (insertposition >= this._size)
            {
                //throw new BplusTreeException("key too big and leaf is full");
                dosplit = true;
                this.PrepareForSplit();
            }
            else
            {
                // if it's already there then change the value at the current location (duplicate entries not supported).
                if (this._childKeys[insertposition] == null || this._owner.Compare(this._childKeys[insertposition], key) == 0) // this.ChildKeys[insertposition].Equals(key)
                {
                    this._childBufferNumbers[insertposition] = position;
                    this._childKeys[insertposition] = key;
                    if (insertposition == 0)
                    {
                        return key;
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            // check for a null position
            int nullindex = insertposition;
            while (nullindex < this._childKeys.Length && this._childKeys[nullindex] != null)
            {
                nullindex++;
            }
            if (nullindex >= this._childKeys.Length)
            {
                if (dosplit)
                {
                    throw new BplusTreeException("can't split twice!!");
                }
                //throw new BplusTreeException("no space in leaf");
                dosplit = true;
                this.PrepareForSplit();
            }
            // bubble in the new info XXXX THIS SHOULD BUBBLE BACKWARDS	
            string nextkey = this._childKeys[insertposition];
            long nextposition = this._childBufferNumbers[insertposition];
            this._childKeys[insertposition] = key;
            this._childBufferNumbers[insertposition] = position;
            while (nextkey != null)
            {
                key = nextkey;
                position = nextposition;
                insertposition++;
                nextkey = this._childKeys[insertposition];
                nextposition = this._childBufferNumbers[insertposition];
                this._childKeys[insertposition] = key;
                this._childBufferNumbers[insertposition] = position;
            }
            // split if needed
            if (dosplit)
            {
                int splitpoint = this._childKeys.Length / 2;
                int splitlength = this._childKeys.Length - splitpoint;
                splitNode = new BplusNode(this._owner, this._parent, -1, this.IsLeaf);
                // copy the split info into the splitNode
                Array.Copy(this._childBufferNumbers, splitpoint, splitNode._childBufferNumbers, 0, splitlength);
                Array.Copy(this._childKeys, splitpoint, splitNode._childKeys, 0, splitlength);
                Array.Copy(this._materializedChildNodes, splitpoint, splitNode._materializedChildNodes, 0, splitlength);
                splitString = splitNode._childKeys[0];
                // archive the new node
                splitNode.DumpToFreshBuffer();
                // store the node data temporarily
                long[] buffernumbers = this._childBufferNumbers;
                string[] keys = this._childKeys;
                BplusNode[] nodes = this._materializedChildNodes;
                // repair current node, copy in the other part of the split
                this._childBufferNumbers = new long[this._size + 1];
                this._childKeys = new string[this._size];
                this._materializedChildNodes = new BplusNode[this._size + 1];
                Array.Copy(buffernumbers, 0, this._childBufferNumbers, 0, splitpoint);
                Array.Copy(keys, 0, this._childKeys, 0, splitpoint);
                Array.Copy(nodes, 0, this._materializedChildNodes, 0, splitpoint);
                for (int i = splitpoint; i < this._childKeys.Length; i++)
                {
                    this._childKeys[i] = null;
                    this._childBufferNumbers[i] = BplusTreeLong.NullBufferNumber;
                    this._materializedChildNodes[i] = null;
                }
                // store the new node
                //splitNode.DumpToFreshBuffer();
                this._owner.RecordTerminalNode(splitNode);
                splitNode.Soil();
            }
            //return this.ChildKeys[0];
            if (insertposition == 0)
            {
                return key; // smallest key changed.
            }
            else
            {
                return null; // no change in smallest key
            }
        }
        /// <summary>
        /// Grow to this.size+1 in preparation for insertion and split
        /// </summary>
        void PrepareForSplit()
        {
            int supersize = this._size + 1;
            long[] positions = new long[supersize + 1];
            string[] keys = new string[supersize];
            BplusNode[] materialized = new BplusNode[supersize + 1];
            Array.Copy(this._childBufferNumbers, 0, positions, 0, this._size + 1);
            positions[this._size + 1] = BplusTreeLong.NullBufferNumber;
            Array.Copy(this._childKeys, 0, keys, 0, this._size);
            keys[this._size] = null;
            Array.Copy(this._materializedChildNodes, 0, materialized, 0, this._size + 1);
            materialized[this._size + 1] = null;
            this._childBufferNumbers = positions;
            this._childKeys = keys;
            this._materializedChildNodes = materialized;
        }
        public void Load(byte[] buffer)
        {
            // load serialized data
            // indicator | seek position | [ key storage | seek position ]*
            this.Clear();
            if (buffer.Length != _owner.Buffersize)
            {
                throw new BplusTreeException("bad buffer size " + buffer.Length + " should be " + _owner.Buffersize);
            }
            byte indicator = buffer[0];
            this.IsLeaf = false;
            if (indicator == BplusTreeLong.Leaf)
            {
                this.IsLeaf = true;
            }
            else if (indicator != BplusTreeLong.NonLeaf)
            {
                throw new BplusTreeException("bad indicator, not leaf or nonleaf in tree " + indicator);
            }
            int index = 1;
            // get the first seek position
            this._childBufferNumbers[0] = BufferFile.RetrieveLong(buffer, index);
            System.Text.Decoder decode = System.Text.Encoding.UTF8.GetDecoder();
            index += BufferFile.LONGSTORAGE;
            int maxKeyLength = this._owner.KeyLength;
            int maxKeyPayload = maxKeyLength - BufferFile.SHORTSTORAGE;
            //this.NumberOfValidKids = 0;
            // get remaining key storages and seek positions
            string lastkey = "";
            for (int KeyIndex = 0; KeyIndex < this._size; KeyIndex++)
            {
                // decode and store a key
                short keylength = BufferFile.RetrieveShort(buffer, index);
                if (keylength < -1 || keylength > maxKeyPayload)
                {
                    throw new BplusTreeException("invalid keylength decoded");
                }
                index += BufferFile.SHORTSTORAGE;
                string key = null;
                if (keylength == 0)
                {
                    key = "";
                }
                else if (keylength > 0)
                {
                    int charCount = decode.GetCharCount(buffer, index, keylength);
                    char[] ca = new char[charCount];
                    decode.GetChars(buffer, index, keylength, ca, 0);
                    //this.NumberOfValidKids++;
                    key = new String(ca);
                }
                this._childKeys[KeyIndex] = key;
                index += maxKeyPayload;
                // decode and store a seek position
                long seekPosition = BufferFile.RetrieveLong(buffer, index);
                if (!this.IsLeaf)
                {
                    if (key == null & seekPosition != BplusTreeLong.NullBufferNumber)
                    {
                        throw new BplusTreeException("key is null but position is not " + KeyIndex);
                    }
                    else if (lastkey == null && key != null)
                    {
                        throw new BplusTreeException("null key followed by non-null key " + KeyIndex);
                    }
                }
                lastkey = key;
                this._childBufferNumbers[KeyIndex + 1] = seekPosition;
                index += BufferFile.LONGSTORAGE;
            }
        }
        /// <summary>
        /// check that key is ok for node of this size (put here for locality of relevant code).
        /// </summary>
        /// <param name="key">key to check</param>
        /// <param name="owner">tree to contain node containing the key</param>
        /// <returns>true if key is ok</returns>
        public static bool KeyOK(string key, BplusTreeLong owner)
        {
            if (key == null)
            {
                return false;
            }
            System.Text.Encoder encode = System.Text.Encoding.UTF8.GetEncoder();
            int maxKeyLength = owner.KeyLength;
            int maxKeyPayload = maxKeyLength - BufferFile.SHORTSTORAGE;
            char[] keyChars = key.ToCharArray();
            int charCount = encode.GetByteCount(keyChars, 0, keyChars.Length, true);
            if (charCount > maxKeyPayload)
            {
                return false;
            }
            return true;
        }
        public void Dump(byte[] buffer)
        {
            // indicator | seek position | [ key storage | seek position ]*
            if (buffer.Length != _owner.Buffersize)
            {
                throw new BplusTreeException("bad buffer size " + buffer.Length + " should be " + _owner.Buffersize);
            }
            buffer[0] = BplusTreeLong.NonLeaf;
            if (this.IsLeaf) { buffer[0] = BplusTreeLong.Leaf; }
            int index = 1;
            // store first seek position
            BufferFile.Store(this._childBufferNumbers[0], buffer, index);
            index += BufferFile.LONGSTORAGE;
            System.Text.Encoder encode = System.Text.Encoding.UTF8.GetEncoder();
            // store remaining keys and seeks
            int maxKeyLength = this._owner.KeyLength;
            int maxKeyPayload = maxKeyLength - BufferFile.SHORTSTORAGE;
            string lastkey = "";
            for (int keyIndex = 0; keyIndex < this._size; keyIndex++)
            {
                // store a key
                string theKey = this._childKeys[keyIndex];
                short charCount = -1;
                if (theKey != null)
                {
                    char[] keyChars = theKey.ToCharArray();
                    charCount = (short)encode.GetByteCount(keyChars, 0, keyChars.Length, true);
                    if (charCount > maxKeyPayload)
                    {
                        throw new BplusTreeException("string bytes to large for use as key " + charCount + ">" + maxKeyPayload);
                    }
                    BufferFile.Store(charCount, buffer, index);
                    index += BufferFile.SHORTSTORAGE;
                    encode.GetBytes(keyChars, 0, keyChars.Length, buffer, index, true);
                }
                else
                {
                    // null case (no string to read)
                    BufferFile.Store(charCount, buffer, index);
                    index += BufferFile.SHORTSTORAGE;
                }
                index += maxKeyPayload;
                // store a seek
                long seekPosition = this._childBufferNumbers[keyIndex + 1];
                if (theKey == null && seekPosition != BplusTreeLong.NullBufferNumber && !this.IsLeaf)
                {
                    throw new BplusTreeException("null key paired with non-null location " + keyIndex);
                }
                if (lastkey == null && theKey != null)
                {
                    throw new BplusTreeException("null key followed by non-null key " + keyIndex);
                }
                lastkey = theKey;
                BufferFile.Store(seekPosition, buffer, index);
                index += BufferFile.LONGSTORAGE;
            }
        }
        /// <summary>
        /// Close the node:
        /// invalidate all children, store state if needed, remove materialized self from parent.
        /// </summary>
        public long Invalidate(bool destroyRoot)
        {
            long result = this.MyBufferNumber;
            if (!this.IsLeaf)
            {
                // need to invalidate kids
                for (int i = 0; i < this._size + 1; i++)
                {
                    if (this._materializedChildNodes[i] != null)
                    {
                        // new buffer numbers are recorded automatically
                        this._childBufferNumbers[i] = this._materializedChildNodes[i].Invalidate(true);
                    }
                }
            }
            // store if dirty
            if (this._dirty)
            {
                result = this.DumpToFreshBuffer();
                //				result = this.myBufferNumber;
            }
            // remove from owner archives if present
            this._owner.ForgetTerminalNode(this);
            // remove from parent
            if (this._parent != null && this._indexInParent >= 0)
            {
                this._parent._materializedChildNodes[this._indexInParent] = null;
                this._parent._childBufferNumbers[this._indexInParent] = result; // should be redundant
                this._parent.CheckIfTerminal();
                this._indexInParent = -1;
            }
            // render all structures useless, just in case...
            if (destroyRoot)
            {
                this.Destroy();
            }
            return result;
        }
        /// <summary>
        /// Mark this as dirty and all ancestors too.
        /// </summary>
        void Soil()
        {
            if (this._dirty)
            {
                return; // don't need to do it again
            }
            else
            {
                this._dirty = true;
                if (this._parent != null)
                {
                    this._parent.Soil();
                }
            }
        }
        public void AsHtml(System.Text.StringBuilder sb)
        {
            string hygeine = "clean";
            if (this._dirty) { hygeine = "dirty"; }
            int keycount = 0;
            if (this.IsLeaf)
            {
                for (int i = 0; i < this._size; i++)
                {
                    string key = this._childKeys[i];
                    long seek = this._childBufferNumbers[i];
                    if (key != null)
                    {
                        key = PrintableString(key);
                        sb.Append("'" + key + "' : " + seek + "<br>\r\n");
                        keycount++;
                    }
                }
                sb.Append("leaf " + this._indexInParent + " at " + this.MyBufferNumber + " #keys==" + keycount + " " + hygeine + "\r\n");
            }
            else
            {
                sb.Append("<table border>\r\n");
                sb.Append("<tr><td colspan=2>nonleaf " + this._indexInParent + " at " + this.MyBufferNumber + " " + hygeine + "</td></tr>\r\n");
                if (this._childBufferNumbers[0] != BplusTreeLong.NullBufferNumber)
                {
                    this.MaterializeNodeAtIndex(0);
                    sb.Append("<tr><td></td><td>" + this._childBufferNumbers[0] + "</td><td>\r\n");
                    this._materializedChildNodes[0].AsHtml(sb);
                    sb.Append("</td></tr>\r\n");
                }
                for (int i = 0; i < this._size; i++)
                {
                    string key = this._childKeys[i];
                    if (key == null)
                    {
                        break;
                    }
                    key = PrintableString(key);
                    sb.Append("<tr><th>'" + key + "'</th><td></td><td></td></tr>\r\n");
                    try
                    {
                        this.MaterializeNodeAtIndex(i + 1);
                        sb.Append("<tr><td></td><td>" + this._childBufferNumbers[i + 1] + "</td><td>\r\n");
                        this._materializedChildNodes[i + 1].AsHtml(sb);
                        sb.Append("</td></tr>\r\n");
                    }
                    catch (BplusTreeException)
                    {
                        sb.Append("<tr><td></td><th>COULDN'T MATERIALIZE NODE " + (i + 1) + "</th></tr>");
                    }
                    keycount++;
                }
                sb.Append("<tr><td colspan=2> #keys==" + keycount + "</td></tr>\r\n");
                sb.Append("</table>\r\n");
            }
        }
        public static string PrintableString(string s)
        {
            if (s == null) { return "[NULL]"; }
            var sb = new System.Text.StringBuilder();
            foreach (char c in s)
            {
                if (Char.IsLetterOrDigit(c) || Char.IsPunctuation(c))
                {
                    sb.Append(c);
                }
                else
                {
                    sb.Append("[" + Convert.ToInt32(c) + "]");
                }
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// Generic error including programming errors.
    /// </summary>
    public class BplusTreeException : ApplicationException
    {
        public BplusTreeException(string message)
            : base(message)
        {
            // do nothing extra
        }
    }
    /// <summary>
    /// No such key found for attempted retrieval.
    /// </summary>
    public class BplusTreeKeyMissing : ApplicationException
    {
        public BplusTreeKeyMissing(string message)
            : base(message)
        {
            // do nothing extra
        }
    }
    /// <summary>
    /// Key cannot be null or too large.
    /// </summary>
    public class BplusTreeBadKeyValue : ApplicationException
    {
        public BplusTreeBadKeyValue(string message)
            : base(message)
        {
            // do nothing extra
        }
    }
}
