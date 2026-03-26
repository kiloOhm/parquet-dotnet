using System;
using System.Text;
using Parquet.Bloom;
using Xunit;

namespace Parquet.Test.Bloom {
    public sealed class SplitBlockBloomFilterTest {
        private const int BlockBytes = 32;

        [Fact]
        public void InsertAndCheck_WithReadableStrings() {
            const int blocks = 4;
            SplitBlockBloomFilter f = new SplitBlockBloomFilter(blocks);

            byte[] aliceBytes = Encoding.UTF8.GetBytes("Alice");
            byte[] bobBytes = Encoding.UTF8.GetBytes("Bob");
            byte[] carolBytes = Encoding.UTF8.GetBytes("Carol");
            byte[] daveBytes = Encoding.UTF8.GetBytes("Dave");

            Assert.False(f.MightContain(aliceBytes));
            Assert.False(f.MightContain(bobBytes));

            f.Insert(bobBytes);
            f.Insert(daveBytes);

            Assert.True(f.MightContain(bobBytes));
            Assert.True(f.MightContain(daveBytes));

            Assert.False(f.MightContain(aliceBytes));
            Assert.False(f.MightContain(carolBytes));
        }

        [Fact]
        public void ToBytes_FromBytes_RoundTrip_WithStrings() {
            const int blocks = 3;
            SplitBlockBloomFilter f1 = new SplitBlockBloomFilter(blocks);

            string[] words = { "Alpha", "Beta", "Gamma" };
            foreach(string w in words) {
                f1.Insert(Encoding.UTF8.GetBytes(w));
            }

            byte[] bytes = f1.ToByteArray();
            Assert.Equal(SplitBlockBloomFilter.BytesForBlocks(blocks), bytes.Length);

            SplitBlockBloomFilter f2 = SplitBlockBloomFilter.FromByteArray(blocks, bytes);

            foreach(string w in words) {
                Assert.True(f2.MightContain(Encoding.UTF8.GetBytes(w)));
            }
        }

        [Fact]
        public void BytesForBlocks_And_Bounds() {
            Assert.Equal(BlockBytes * 1, SplitBlockBloomFilter.BytesForBlocks(1));
            Assert.Equal(BlockBytes * 2, SplitBlockBloomFilter.BytesForBlocks(2));
            Assert.Equal(BlockBytes * 10, SplitBlockBloomFilter.BytesForBlocks(10));

            Assert.Throws<ArgumentOutOfRangeException>(() => new SplitBlockBloomFilter(0));
            Assert.Throws<ArgumentOutOfRangeException>(() => SplitBlockBloomFilter.BytesForBlocks(0));
            Assert.Throws<ArgumentOutOfRangeException>(() => SplitBlockBloomFilter.FromByteArray(0, Array.Empty<byte>()));
        }

        [Fact]
        public void MapHashToBlock_Uses_MostSignificant_32_Bits_Per_Spec() {
            ulong hash = 0x12345678_9ABCDEF0UL;
            int blocks = 17;

            Assert.Equal(1, SplitBlockBloomFilter.MapHashToBlock(hash, blocks));
        }

        [Fact]
        public void ComputeMasksFor_Matches_Spec_Bit_Selection() {
            uint[] masks = SplitBlockBloomFilter.ComputeMasksFor(0x9ABCDEF0U);

            Assert.Equal(new uint[] {
                0x00002000U,
                0x00400000U,
                0x00008000U,
                0x00100000U,
                0x00000800U,
                0x00000004U,
                0x00000800U,
                0x00000080U
            }, masks);
        }
    }
}
