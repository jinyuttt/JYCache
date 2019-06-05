using System;
using System.Collections;

namespace BPlusTree
{
    /// <summary>
    /// Chunked singly linked file with garbage collection.
    /// </summary>
    public class LinkedFile
    {
        const long NULLBUFFERPOINTER = -1;
        System.IO.Stream fromfile;
        BufferFile buffers;
        int buffersize;
        int headersize;
        long seekStart = 0;
        long FreeListHead = NULLBUFFERPOINTER;
        long RecentNewBufferNumber = NULLBUFFERPOINTER;
        bool headerDirty = true;
        byte FREE = 0;
        byte HEAD = 1;
        byte BODY = 2;
        public static byte[] HEADERPREFIX = { 98, 112, 78, 108, 102 };
        public static byte VERSION = 0;
        public static int MINBUFFERSIZE = 20;
        // next pointer and indicator flag
        public static int BUFFEROVERHEAD = BufferFile.LONGSTORAGE + 1;
        public LinkedFile(int buffersize, long seekStart)
        {
            this.seekStart = seekStart;
            //this.buffers = buffers;
            this.buffersize = buffersize;
            // markers+version byte+buffersize+freelisthead
            this.headersize = HEADERPREFIX.Length + 1 + BufferFile.INTSTORAGE + BufferFile.LONGSTORAGE;
            this.SanityCheck();
        }
        public static LinkedFile SetupFromExistingStream(System.IO.Stream fromfile)
        {
            return SetupFromExistingStream(fromfile, (long)0);
        }
        public static LinkedFile SetupFromExistingStream(System.IO.Stream fromfile, long StartSeek)
        {
            LinkedFile result = new LinkedFile(100, StartSeek); // dummy buffer size for now
            result.fromfile = fromfile;
            result.ReadHeader();
            result.buffers = BufferFile.SetupFromExistingStream(fromfile, StartSeek + result.headersize);
            return result;
        }
        void ReadHeader()
        {
            byte[] header = new byte[this.headersize];
            this.fromfile.Seek(this.seekStart, System.IO.SeekOrigin.Begin);
            this.fromfile.Read(header, 0, this.headersize);
            int index = 0;
            // check prefix
            foreach (byte b in HEADERPREFIX)
            {
                if (header[index] != b)
                {
                    throw new LinkedFileException("invalid header prefix");
                }
                index++;
            }
            // skip version (for now)
            index++;
            // read buffersize
            this.buffersize = BufferFile.Retrieve(header, index);
            index += BufferFile.INTSTORAGE;
            this.FreeListHead = BufferFile.RetrieveLong(header, index);
            this.SanityCheck();
            this.headerDirty = false;
        }
        public static LinkedFile InitializeLinkedFileInStream(System.IO.Stream fromfile, int buffersize)
        {
            return InitializeLinkedFileInStream(fromfile, buffersize, (long)0);
        }
        public static LinkedFile InitializeLinkedFileInStream(System.IO.Stream fromfile, int buffersize, long startSeek)
        {
            LinkedFile result = new LinkedFile(buffersize, startSeek);
            result.fromfile = fromfile;
            result.SetHeader();
            // buffersize should be increased by overhead...
            result.buffers = BufferFile.InitializeBufferFileInStream(fromfile, buffersize + BUFFEROVERHEAD, startSeek + result.headersize);
            return result;
        }
        public void SetHeader()
        {
            byte[] header = this.MakeHeader();
            this.fromfile.Seek(this.seekStart, System.IO.SeekOrigin.Begin);
            this.fromfile.Write(header, 0, header.Length);
            this.headerDirty = false;
        }
        public byte[] MakeHeader()
        {
            byte[] result = new byte[this.headersize];
            HEADERPREFIX.CopyTo(result, 0);
            result[HEADERPREFIX.Length] = VERSION;
            int index = HEADERPREFIX.Length + 1;
            BufferFile.Store(this.buffersize, result, index);
            index += BufferFile.INTSTORAGE;
            BufferFile.Store(this.FreeListHead, result, index);
            return result;
        }
        public void Recover(Hashtable chunksInUse, bool fixErrors)
        {
            // find missing space and recover it
            this.CheckStructure(chunksInUse, fixErrors);
        }
        void SanityCheck()
        {
            if (this.seekStart < 0)
            {
                throw new LinkedFileException("cannot seek negative " + this.seekStart);
            }
            if (this.buffersize < MINBUFFERSIZE)
            {
                throw new LinkedFileException("buffer size too small " + this.buffersize);
            }
        }
        public void Shutdown()
        {
            this.fromfile.Flush();
            this.fromfile.Close();
        }
        byte[] ParseBuffer(long bufferNumber, out byte type, out long nextBufferNumber)
        {
            byte[] thebuffer = new byte[this.buffersize];
            byte[] fullbuffer = new byte[this.buffersize + BUFFEROVERHEAD];
            this.buffers.GetBuffer(bufferNumber, fullbuffer, 0, fullbuffer.Length);
            type = fullbuffer[0];
            nextBufferNumber = BufferFile.RetrieveLong(fullbuffer, 1);
            Array.Copy(fullbuffer, BUFFEROVERHEAD, thebuffer, 0, this.buffersize);
            return thebuffer;
        }
        void SetBuffer(long buffernumber, byte type, byte[] thebuffer, int start, int length, long nextBufferNumber)
        {
            //System.Diagnostics.Debug.WriteLine(" storing chunk type "+type+" at "+buffernumber);
            if (this.buffersize < length)
            {
                throw new LinkedFileException("buffer size too small " + this.buffersize + "<" + length);
            }
            byte[] fullbuffer = new byte[length + BUFFEROVERHEAD];
            fullbuffer[0] = type;
            BufferFile.Store(nextBufferNumber, fullbuffer, 1);
            if (thebuffer != null)
            {
                Array.Copy(thebuffer, start, fullbuffer, BUFFEROVERHEAD, length);
            }
            this.buffers.SetBuffer(buffernumber, fullbuffer, 0, fullbuffer.Length);
        }

        void DeallocateBuffer(long buffernumber)
        {

            //System.Diagnostics.Debug.WriteLine(" deallocating "+buffernumber);
            // should be followed by resetting the header eventually.
            this.SetBuffer(buffernumber, FREE, null, 0, 0, this.FreeListHead);
            this.FreeListHead = buffernumber;
            this.headerDirty = true;
        }
        long AllocateBuffer()
        {
            if (this.FreeListHead != NULLBUFFERPOINTER)
            {
                // reallocate a freed buffer
                long result = this.FreeListHead;
                byte buffertype;
                long nextFree;
                byte[] dummy = this.ParseBuffer(result, out buffertype, out nextFree);
                if (buffertype != FREE)
                {
                    throw new LinkedFileException("free head buffer not marked free");
                }
                this.FreeListHead = nextFree;
                this.headerDirty = true;
                this.RecentNewBufferNumber = NULLBUFFERPOINTER;
                return result;
            }
            else
            {
                // allocate a new buffer
                long nextbuffernumber = this.buffers.NextBufferNumber();
                if (this.RecentNewBufferNumber == nextbuffernumber)
                {
                    // the previous buffer has been allocated but not yet written.  It must be written before the following one...
                    nextbuffernumber++;
                }
                this.RecentNewBufferNumber = nextbuffernumber;
                return nextbuffernumber;
            }
        }
        public void CheckStructure()
        {
            CheckStructure(null, false);
        }
        public void CheckStructure(Hashtable chunksInUse, bool fixErrors)
        {
            Hashtable buffernumberToType = new Hashtable();
            Hashtable buffernumberToNext = new Hashtable();
            Hashtable visited = new Hashtable();
            long lastBufferNumber = this.buffers.NextBufferNumber();
            for (long buffernumber = 0; buffernumber < lastBufferNumber; buffernumber++)
            {
                byte buffertype;
                long nextBufferNumber;
                this.ParseBuffer(buffernumber, out buffertype, out nextBufferNumber);
                buffernumberToType[buffernumber] = buffertype;
                buffernumberToNext[buffernumber] = nextBufferNumber;
            }
            // traverse the freelist
            long thisFreeBuffer = this.FreeListHead;
            while (thisFreeBuffer != NULLBUFFERPOINTER)
            {
                if (visited.ContainsKey(thisFreeBuffer))
                {
                    throw new LinkedFileException("cycle in freelist " + thisFreeBuffer);
                }
                visited[thisFreeBuffer] = thisFreeBuffer;
                byte thetype = (byte)buffernumberToType[thisFreeBuffer];
                long nextbuffernumber = (long)buffernumberToNext[thisFreeBuffer];
                if (thetype != FREE)
                {
                    throw new LinkedFileException("free list element not marked free " + thisFreeBuffer);
                }
                thisFreeBuffer = nextbuffernumber;
            }
            // traverse all nodes marked head
            Hashtable allchunks = new Hashtable();
            for (long buffernumber = 0; buffernumber < lastBufferNumber; buffernumber++)
            {
                byte thetype = (byte)buffernumberToType[buffernumber];
                if (thetype == HEAD)
                {
                    if (visited.ContainsKey(buffernumber))
                    {
                        throw new LinkedFileException("head buffer already visited " + buffernumber);
                    }
                    allchunks[buffernumber] = buffernumber;
                    visited[buffernumber] = buffernumber;
                    long bodybuffernumber = (long)buffernumberToNext[buffernumber];
                    while (bodybuffernumber != NULLBUFFERPOINTER)
                    {
                        byte bodytype = (byte)buffernumberToType[bodybuffernumber];
                        long nextbuffernumber = (long)buffernumberToNext[bodybuffernumber];
                        if (visited.ContainsKey(bodybuffernumber))
                        {
                            throw new LinkedFileException("body buffer visited twice " + bodybuffernumber);
                        }
                        visited[bodybuffernumber] = bodytype;
                        if (bodytype != BODY)
                        {
                            throw new LinkedFileException("body buffer not marked body " + thetype);
                        }
                        bodybuffernumber = nextbuffernumber;
                    }
                    // check retrieval
                    this.GetChunk(buffernumber);
                }
            }
            // make sure all were visited
            for (long buffernumber = 0; buffernumber < lastBufferNumber; buffernumber++)
            {
                if (!visited.ContainsKey(buffernumber))
                {
                    throw new LinkedFileException("buffer not found either as data or free " + buffernumber);
                }
            }
            // check against in use list
            if (chunksInUse != null)
            {
                ArrayList notInUse = new ArrayList();
                foreach (DictionaryEntry d in chunksInUse)
                {
                    long buffernumber = (long)d.Key;
                    if (!allchunks.ContainsKey(buffernumber))
                    {
                        //System.Diagnostics.Debug.WriteLine("\r\n<br>allocated chunks "+allchunks.Count);
                        //foreach (DictionaryEntry d1 in allchunks) 
                        //{
                        //	System.Diagnostics.Debug.WriteLine("\r\n<br>found "+d1.Key);
                        //}
                        throw new LinkedFileException("buffer in used list not found in linked file " + buffernumber + " " + d.Value);
                    }
                }
                foreach (DictionaryEntry d in allchunks)
                {
                    long buffernumber = (long)d.Key;
                    if (!chunksInUse.ContainsKey(buffernumber))
                    {
                        if (!fixErrors)
                        {
                            throw new LinkedFileException("buffer in linked file not in used list " + buffernumber);
                        }
                        notInUse.Add(buffernumber);
                    }
                }
                notInUse.Sort();
                notInUse.Reverse();
                foreach (object thing in notInUse)
                {
                    long buffernumber = (long)thing;
                    this.ReleaseBuffers(buffernumber);
                }
            }
        }
        public byte[] GetChunk(long headBufferNumber)
        {
            // get the head, interpret the length
            byte buffertype;
            long nextBufferNumber;
            byte[] buffer = this.ParseBuffer(headBufferNumber, out buffertype, out nextBufferNumber);
            int length = BufferFile.Retrieve(buffer, 0);
            if (length < 0)
            {
                throw new LinkedFileException("negative length block? must be garbage: " + length);
            }
            if (buffertype != HEAD)
            {
                throw new LinkedFileException("first buffer not marked HEAD");
            }
            byte[] result = new byte[length];
            // read in the data from the first buffer
            int firstLength = this.buffersize - BufferFile.INTSTORAGE;
            if (firstLength > length)
            {
                firstLength = length;
            }
            Array.Copy(buffer, BufferFile.INTSTORAGE, result, 0, firstLength);
            int stored = firstLength;
            while (stored < length)
            {
                // get the next buffer
                long thisBufferNumber = nextBufferNumber;
                buffer = this.ParseBuffer(thisBufferNumber, out buffertype, out nextBufferNumber);
                int nextLength = this.buffersize;
                if (length - stored < nextLength)
                {
                    nextLength = length - stored;
                }
                Array.Copy(buffer, 0, result, stored, nextLength);
                stored += nextLength;
            }
            return result;
        }
        public long StoreNewChunk(byte[] fromArray, int startingAt, int length)
        {
            // get the first buffer as result value
            long currentBufferNumber = this.AllocateBuffer();
            //System.Diagnostics.Debug.WriteLine(" allocating chunk starting at "+currentBufferNumber);
            long result = currentBufferNumber;
            if (length < 0 || startingAt < 0)
            {
                throw new LinkedFileException("cannot store negative length chunk (" + startingAt + "," + length + ")");
            }
            int endingAt = startingAt + length;
            // special case: zero length chunk
            if (endingAt > fromArray.Length)
            {
                throw new LinkedFileException("array doesn't have this much data: " + endingAt);
            }
            int index = startingAt;
            // store header with length information
            byte[] buffer = new byte[this.buffersize];
            BufferFile.Store(length, buffer, 0);
            int fromIndex = startingAt;
            int firstLength = this.buffersize - BufferFile.INTSTORAGE;
            int stored = 0;
            if (firstLength > length)
            {
                firstLength = length;
            }
            Array.Copy(fromArray, fromIndex, buffer, BufferFile.INTSTORAGE, firstLength);
            stored += firstLength;
            fromIndex += firstLength;
            byte currentBufferType = HEAD;
            // store any remaining buffers (no length info)
            while (stored < length)
            {
                // store current buffer and get next block number
                long nextBufferNumber = this.AllocateBuffer();
                this.SetBuffer(currentBufferNumber, currentBufferType, buffer, 0, buffer.Length, nextBufferNumber);
                currentBufferNumber = nextBufferNumber;
                currentBufferType = BODY;
                int nextLength = this.buffersize;
                if (stored + nextLength > length)
                {
                    nextLength = length - stored;
                }
                Array.Copy(fromArray, fromIndex, buffer, 0, nextLength);
                stored += nextLength;
                fromIndex += nextLength;
            }
            // store final buffer
            this.SetBuffer(currentBufferNumber, currentBufferType, buffer, 0, buffer.Length, NULLBUFFERPOINTER);
            return result;
        }
        public void Flush()
        {
            if (this.headerDirty)
            {
                this.SetHeader();
            }
            this.buffers.Flush();
        }
        public void ReleaseBuffers(long headBufferNumber)
        {
            // KISS
            //System.Diagnostics.Debug.WriteLine(" deallocating chunk starting at "+HeadBufferNumber);
            long thisbuffernumber = headBufferNumber;
            long nextbuffernumber;
            byte buffertype;
            byte[] dummy = this.ParseBuffer(headBufferNumber, out buffertype, out nextbuffernumber);
            if (buffertype != HEAD)
            {
                throw new LinkedFileException("head buffer not marked HEAD");
            }
            this.DeallocateBuffer(headBufferNumber);
            while (nextbuffernumber != NULLBUFFERPOINTER)
            {
                thisbuffernumber = nextbuffernumber;
                dummy = this.ParseBuffer(thisbuffernumber, out buffertype, out nextbuffernumber);
                if (buffertype != BODY)
                {
                    throw new LinkedFileException("body buffer not marked BODY");
                }
                this.DeallocateBuffer(thisbuffernumber);
            }
        }
    }
    public class LinkedFileException : ApplicationException
    {
        public LinkedFileException(string message)
            : base(message)
        {
            // do nothing extra
        }
    }
}
