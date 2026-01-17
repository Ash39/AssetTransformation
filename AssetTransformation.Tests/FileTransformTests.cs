using StbImageSharp;
using System.Security.Cryptography;
using System.Text;
using static System.Net.Mime.MediaTypeNames;

namespace AssetTransformation.Tests
{
    public class FileTransformTests : IDisposable
    {
        private readonly string _assetsPath;
        private readonly string _appName;
        private readonly List<string> _tempFiles;

        public FileTransformTests()
        {
            _assetsPath = Path.Combine(AppContext.BaseDirectory, "Assets");
            _appName = "FileTransformTests_" + Guid.NewGuid(); // isolate per test run
            _tempFiles = new List<string>();
        }

        private string[] GetAssetFiles()
        {
            return Directory.GetFiles(_assetsPath);
        }

        private string CreateTempFile(string name, string content)
        {
            string dir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(dir);

            string path = Path.Combine(dir, name);
            File.WriteAllText(path, content);
            _tempFiles.Add(path);
            return path;
        }

        private byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

        public void Dispose()
        {
            // cleanup app data folder
            var appRoot = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                _appName);

            foreach (var file in _tempFiles) 
            {
                File.Delete(file);
            }

            if (Directory.Exists(appRoot))
            {
                Directory.Delete(appRoot, true);
            }
        }

        [Fact]
        public void RealisticTest()
        {
            FileTransform fileTransform = new FileTransform(_appName, GetAssetFiles());

            FileTransform result = fileTransform.Split("\\.(png|jpg)$", (imageTransform) => 
            {
                return imageTransform.SelectMultiThread("StbImageLoad", (source, name, ref sender) =>
                {
                    ImageResult image = ImageResult.FromMemory(source);

                    sender = (image.Width, image.Height, image.SourceComp);

                    return image.Data;
                })
                .Select("FlipImage", (source, name, ref sender) =>
                {
                    (int Width, int Height, ColorComponents comp) = ((int, int, ColorComponents))sender;

                    byte[] copiedBytes = new byte[source.Length];

                    Array.Copy(source, copiedBytes, source.Length);

                    unsafe
                    {
                        fixed (void* data = copiedBytes)
                        {
                            StbImageSharp.StbImage.stbi__vertical_flip(data, Width, Height, (int)comp);
                        }
                    }

                    Assert.Equal(copiedBytes.Length, source.Length);
                    Assert.NotEqual(source, copiedBytes);

                    return copiedBytes;
                });

            }, nonMatch => nonMatch);

            Assert.Equal(fileTransform.Results.Length, result.Results.Length);
        }

        [Fact]
        public void Split_AppliesMatchAndNonMatch_AndConcatenatesInOrder()
        {
            var file1 = CreateTempFile("a.txt", "A");
            var file2 = CreateTempFile("b.bin", "B");

            var transform = new FileTransform(_appName, new[] { file1, file2 });

            var result = transform.Split(
                pattern: @"\.txt$",
                match: t => t.Select("SplitMatch", (b, _, ref _) => Bytes("M")),
                nonMatch: t => t.Select("SplitNon", (b, _, ref _) => Bytes("N"))
            );

            var outputs = result.Results;

            Assert.Equal(2, outputs.Length);
            Assert.Equal("M", Encoding.UTF8.GetString(outputs[0].Item2));
            Assert.Equal("N", Encoding.UTF8.GetString(outputs[1].Item2));
        }

        [Fact]
        public void Split_PropagatesExceptionsFromTasks()
        {
            var file = CreateTempFile("a.txt", "A");
            var transform = new FileTransform(_appName, new[] { file });

            Assert.ThrowsAny<Exception>(() =>
                transform.Split(
                    @"\.txt$",
                    _ => throw new InvalidOperationException("boom"),
                    t => t
                )
            );
        }

        [Fact]
        public void SplitBy_GroupsFilesAndConcatenatesInFuncOrder()
        {
            var f1 = CreateTempFile("a.txt", "A");
            var f2 = CreateTempFile("b.txt", "B");
            var f3 = CreateTempFile("c.bin", "C");

            var transform = new FileTransform(_appName, new[] { f1, f2, f3 });

            var result = transform.SplitBy(
                keySelector: (bytes, name) =>
                    name.EndsWith(".txt") ? "text" : "bin",

                ("bin", t => t.Select("SplitByBin", (b, _, ref _) => Bytes("B"))),
                ("text", t => t.Select("SplitByText", (b, _, ref _) => Bytes("T")))
            );

            var outputs = result.Results;

            Assert.Equal(3, outputs.Length);

            Assert.Equal("B", Encoding.UTF8.GetString(outputs[0].Item2));
            Assert.Equal("T", Encoding.UTF8.GetString(outputs[1].Item2));
            Assert.Equal("T", Encoding.UTF8.GetString(outputs[2].Item2));
        }

        [Fact]
        public void SplitBy_ThrowsIfGroupKeyMissing()
        {
            var f1 = CreateTempFile("a.txt", "A");
            var transform = new FileTransform(_appName, new[] { f1 });

            Assert.Throws<KeyNotFoundException>(() =>
                transform.SplitBy(
                    (b, n) => "text",
                    ("missing", t => t)
                )
            );
        }

        [Fact]
        public void SplitBy_ThrowsIfFuncIsNull()
        {
            var f1 = CreateTempFile("a.txt", "A");
            var transform = new FileTransform(_appName, new[] { f1 });

            Assert.Throws<ArgumentNullException>(() =>
                transform.SplitBy(
                    (b, n) => "text",
                    ("text", null!)
                )
            );
        }

        [Fact]
        public void Constructor_LoadsFilesCorrectly()
        {
            var files = GetAssetFiles();

            var ft = new FileTransform(_appName, files);

            var results = ft.Results;

            Assert.Equal(files.Length, results.Length);

            foreach (var (info, bytes, sender) in results)
            {
                string originalPath = files.First(f => Path.GetFileName(f) == info.Name);
                byte[] originalBytes = File.ReadAllBytes(originalPath);

                Assert.Equal(originalBytes, bytes);
            }
        }

        [Fact]
        public void Select_TransformsAndCaches()
        {
            var files = GetAssetFiles();

            var ft = new FileTransform(_appName, files);

            int callCount = 0;

            byte[] Transform(byte[] data, FileInfo info, ref object sender)
            {
                return data.Reverse().ToArray();
            }

            ft.CacheMiss += () => callCount++;

            var stage1 = ft.Select("reverse", Transform);

            // first run should call transform for each file
            Assert.Equal(files.Length, callCount);

            // second run should hit cache
            var stage2 = ft.Select("reverse", Transform);

            Assert.Equal(files.Length, callCount); // unchanged

            var results = stage1.Results;
            for (int i = 0; i < results.Length; i++)
            {
                var original = File.ReadAllBytes(files[i]);
                Assert.Equal(original.Reverse().ToArray(), results[i].Item2);
            }
        }

        [Fact]
        public void SelectMultiThread_TransformsAndCaches()
        {
            var files = GetAssetFiles();

            var ft = new FileTransform(_appName, files);

            int callCount = 0;

            byte[] Transform(byte[] data, FileInfo info, ref object sender)
            {
                return data.Reverse().ToArray();
            }

            ft.CacheMiss += () => callCount++;

            var stage1 = ft.SelectMultiThread("reverse", Transform);

            // first run should call transform for each file
            Assert.Equal(files.Length, callCount);

            // second run should hit cache
            var stage2 = ft.SelectMultiThread("reverse", Transform);

            Assert.Equal(files.Length, callCount); // unchanged

            var results = stage1.Results;
            for (int i = 0; i < results.Length; i++)
            {
                var original = File.ReadAllBytes(files[i]);
                Assert.Equal(original.Reverse().ToArray(), results[i].Item2);
            }
        }

        [Fact]
        public void SelectMultiThread_ProducesSameResultsAsSelect()
        {
            var files = GetAssetFiles();

            var ft = new FileTransform(_appName, files);

            byte[] Transform(byte[] data, FileInfo info, ref object sender)
            {
                return SHA256.HashData(data);
            }

            var single = ft.Select("hash_single", Transform);
            var multi = ft.SelectMultiThread("hash_multi", Transform);

            var singleResults = single.Results;
            var multiResults = multi.Results;

            Assert.Equal(singleResults.Length, multiResults.Length);

            for (int i = 0; i < singleResults.Length; i++)
            {
                Assert.Equal(singleResults[i].Item1, multiResults[i].Item1);
                Assert.Equal(singleResults[i].Item2, multiResults[i].Item2);
            }
        }

        [Fact]
        public void Where_FiltersCorrectly()
        {
            var files = GetAssetFiles();

            var ft = new FileTransform(_appName, files);

            var txtOnly = ft.Where((bytes, info) => info.Extension.Equals(".txt", StringComparison.OrdinalIgnoreCase));

            Assert.All(txtOnly.Results, r => Assert.Equal(".txt", r.Item1.Extension));
        }

        [Fact]
        public void Concat_CombinesTwoFileTransforms()
        {
            var files = GetAssetFiles();

            var ft1 = new FileTransform(_appName, files.Take(1).ToArray());
            var ft2 = new FileTransform(_appName, files.Skip(1).ToArray());

            var combined = ft1.Concat(ft2);

            Assert.Equal(files.Length, combined.Results.Length);

            var combinedNames = combined.Results.Select(r => r.Item1.Name).ToArray();
            var originalNames = files.Select(Path.GetFileName).ToArray();

            Assert.Equal(originalNames, combinedNames);
        }

        [Fact]
        public void Split_ByRegex_WorksCorrectly()
        {
            var files = GetAssetFiles();

            var ft = new FileTransform(_appName, files);

            var split = ft.Split(@"\.txt$");

            var matches = split[0];
            var nonMatches = split[1];

            Assert.All(matches.Results, r => Assert.Equal(".txt", r.Item1.Extension));
            //Assert.All(nonMatches.Results, r => Assert.EndsWith(".txt", r.Item1, StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void SplitBy_GroupsCorrectly()
        {
            var files = GetAssetFiles();

            var ft = new FileTransform(_appName, files);

            var groups = ft.SplitBy((bytes, name) => Path.GetExtension(name));

            foreach (var kvp in groups)
            {
                string ext = kvp.Key;
                var groupFt = kvp.Value;

                Assert.All(groupFt.Results, r => Assert.Equal(ext, r.Item1.Extension));
            }
        }

        [Fact]
        public void Enumerator_IteratesAllFiles()
        {
            var files = GetAssetFiles();

            var ft = new FileTransform(_appName, files);

            int count = 0;
            foreach (var (name, bytes, sender) in ft)
            {
                Assert.NotNull(name);
                Assert.NotNull(bytes);
                count++;
            }

            Assert.Equal(files.Length, count);
        }

        [Fact]
        public void Results_ReturnsAllFilesInOrder()
        {
            var files = GetAssetFiles();

            var ft = new FileTransform(_appName, files);

            var results = ft.Results;

            Assert.Equal(files.Length, results.Length);

            for (int i = 0; i < files.Length; i++)
            {
                Assert.Equal(Path.GetFileName(files[i]), results[i].Item1.Name);
            }
        }
    }
}
