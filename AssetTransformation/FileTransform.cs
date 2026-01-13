using System.Collections;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace AssetTransformation
{
    public class FileTransform : IEnumerator<(string, byte[])>
    {
        private readonly byte[][] filesBytes;
        private readonly string[] fileNames;
        private readonly string appRootPath;
        private int index = 0;

        public FileTransform(string appName, string[] files)
        {
            if (files == null) throw new ArgumentNullException(nameof(files));

            filesBytes = new byte[files.Length][];
            fileNames = new string[files.Length];

            for (int i = 0; i < files.Length; i++)
            {
                filesBytes[i] = File.ReadAllBytes(files[i]);
                fileNames[i] = Path.GetFileName(files[i]);
            }

            appRootPath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                appName
            );

            Directory.CreateDirectory(appRootPath);
        }

        private FileTransform(string appRootPath, byte[][] filesBytes, string[] fileNames)
        {
            this.appRootPath = appRootPath;
            this.filesBytes = filesBytes;
            this.fileNames = fileNames;
        }


        public FileTransform Select(string stage, Func<byte[], string, byte[]> func)
        {
            if (stage == null) throw new ArgumentNullException(nameof(stage));
            if (func == null) throw new ArgumentNullException(nameof(func));

            string stageFolder = Path.Combine(appRootPath, stage);
            Directory.CreateDirectory(stageFolder);

            byte[][] fileCaches = new byte[filesBytes.Length][];

            for (int i = 0; i < filesBytes.Length; i++)
            {
                byte[] inputBytes = filesBytes[i];
                string fileName = fileNames[i];

                byte[] hash = ComputeTransformHash(inputBytes, fileName, func);
                string cacheName = Convert.ToHexString(hash) + ".bin";
                string cachePath = Path.Combine(stageFolder, cacheName);

                if (!File.Exists(cachePath))
                {
                    byte[] transformed = func(inputBytes, fileName);
                    File.WriteAllBytes(cachePath, transformed);
                }

                fileCaches[i] = File.ReadAllBytes(cachePath);
            }

            return new FileTransform(appRootPath, fileCaches, fileNames);
        }

        /// <param name="func">Must be thread-safe.</param>
        public FileTransform SelectMultiThread(string stage, Func<byte[], string, byte[]> func)
        {
            if (stage == null) throw new ArgumentNullException(nameof(stage));
            if (func == null) throw new ArgumentNullException(nameof(func));

            string stageFolder = Path.Combine(appRootPath, stage);
            Directory.CreateDirectory(stageFolder);

            byte[][] fileCaches = new byte[filesBytes.Length][];

            Parallel.For(
                0,
                filesBytes.Length,
                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                i =>
                {
                    byte[] inputBytes = filesBytes[i];
                    string fileName = fileNames[i];

                    byte[] hash = ComputeTransformHash(inputBytes, fileName, func);
                    string cacheName = Convert.ToHexString(hash) + ".bin";
                    string cachePath = Path.Combine(stageFolder, cacheName);

                    try
                    {
                        using var fs = new FileStream(cachePath, FileMode.CreateNew, FileAccess.Write, FileShare.None);
                        byte[] transformed = func(inputBytes, fileName);
                        fs.Write(transformed, 0, transformed.Length);
                    }
                    catch (IOException)
                    {
                        // Another thread/process already wrote it
                    }

                    fileCaches[i] = File.ReadAllBytes(cachePath);
                });

            return new FileTransform(appRootPath, fileCaches, fileNames);
        }


        public FileTransform Where(Func<byte[], string, bool> predicate)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));

            var filteredBytes = new List<byte[]>();
            var filteredNames = new List<string>();

            for (int i = 0; i < filesBytes.Length; i++)
            {
                if (predicate(filesBytes[i], fileNames[i]))
                {
                    filteredBytes.Add(filesBytes[i]);
                    filteredNames.Add(fileNames[i]);
                }
            }

            return new FileTransform(appRootPath, filteredBytes.ToArray(), filteredNames.ToArray());
        }


        public FileTransform Combine(FileTransform other)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            if (this.appRootPath != other.appRootPath)
                throw new InvalidOperationException("Cannot combine FileTransforms with different app roots.");

            byte[][] combinedBytes = new byte[this.filesBytes.Length + other.filesBytes.Length][];
            string[] combinedNames = new string[this.fileNames.Length + other.fileNames.Length];

            Array.Copy(this.filesBytes, 0, combinedBytes, 0, this.filesBytes.Length);
            Array.Copy(other.filesBytes, 0, combinedBytes, this.filesBytes.Length, other.filesBytes.Length);

            Array.Copy(this.fileNames, 0, combinedNames, 0, this.fileNames.Length);
            Array.Copy(other.fileNames, 0, combinedNames, this.fileNames.Length, other.fileNames.Length);

            return new FileTransform(appRootPath, combinedBytes, combinedNames);
        }

        public FileTransform[] Split(string pattern)
        {
            if (pattern == null) throw new ArgumentNullException(nameof(pattern));

            var regex = new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.Compiled);

            var matchBytes = new List<byte[]>();
            var matchNames = new List<string>();

            var nonMatchBytes = new List<byte[]>();
            var nonMatchNames = new List<string>();

            for (int i = 0; i < filesBytes.Length; i++)
            {
                string name = fileNames[i];
                byte[] bytes = filesBytes[i];

                if (regex.IsMatch(name))
                {
                    matchBytes.Add(bytes);
                    matchNames.Add(name);
                }
                else
                {
                    nonMatchBytes.Add(bytes);
                    nonMatchNames.Add(name);
                }
            }

            return new[]
            {
                new FileTransform(appRootPath, matchBytes.ToArray(), matchNames.ToArray()),
                new FileTransform(appRootPath, nonMatchBytes.ToArray(), nonMatchNames.ToArray())
            };
        }


        public Dictionary<string, FileTransform> SplitBy(Func<byte[], string, string> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));

            var groups = new Dictionary<string, List<(byte[] Bytes, string Name)>>();

            for (int i = 0; i < filesBytes.Length; i++)
            {
                string key = keySelector(filesBytes[i], fileNames[i]) ?? string.Empty;

                if (!groups.TryGetValue(key, out var list))
                {
                    list = new List<(byte[] Bytes, string Name)>();
                    groups[key] = list;
                }

                list.Add((filesBytes[i], fileNames[i]));
            }

            var result = new Dictionary<string, FileTransform>();

            foreach (var kvp in groups)
            {
                var bytes = kvp.Value.Select(v => v.Bytes).ToArray();
                var names = kvp.Value.Select(v => v.Name).ToArray();

                result[kvp.Key] = new FileTransform(appRootPath, bytes, names);
            }

            return result;
        }

        private static byte[] ComputeTransformHash(byte[] data, string fileName, Delegate transform)
        {
            byte[] nameBytes = Encoding.UTF8.GetBytes(fileName);
            byte[] ilHash = GetMethodILHash(transform);
            byte[] closureHash = GetClosureStateHash(transform);

            byte[] combined = new byte[data.Length + nameBytes.Length + ilHash.Length + closureHash.Length];

            int offset = 0;
            Buffer.BlockCopy(data, 0, combined, offset, data.Length);
            offset += data.Length;

            Buffer.BlockCopy(nameBytes, 0, combined, offset, nameBytes.Length);
            offset += nameBytes.Length;

            Buffer.BlockCopy(ilHash, 0, combined, offset, ilHash.Length);
            offset += ilHash.Length;

            Buffer.BlockCopy(closureHash, 0, combined, offset, closureHash.Length);

            return SHA256.HashData(combined);
        }

        private static byte[] GetMethodILHash(Delegate del)
        {
            MethodInfo method = del.Method;
            MethodBody? body = method.GetMethodBody();

            if (body == null)
                return Array.Empty<byte>();

            byte[] il = body.GetILAsByteArray()!;
            return SHA256.HashData(il);
        }

        private static byte[] GetClosureStateHash(Delegate del)
        {
            object? target = del.Target;
            if (target == null)
                return Array.Empty<byte>();

            var fields = target.GetType()
                .GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

            using var ms = new MemoryStream();
            using var bw = new BinaryWriter(ms);

            foreach (var field in fields)
            {
                object? value = field.GetValue(target);
                if (value != null)
                {
                    string s = value.ToString()!;
                    byte[] data = Encoding.UTF8.GetBytes(s);
                    bw.Write(data);
                }
            }

            return SHA256.HashData(ms.ToArray());
        }

        public bool MoveNext()
        {
            int currentIndex = index;
            int nextIndex = currentIndex + 1;
            if (filesBytes.Length < nextIndex)
            {
                index = nextIndex;
                return true;
            }
            return false;
        }

        public void Reset()
        {
            index = 0;
        }

        public void Dispose()
        {
            index = 0;
        }

        public (string, byte[])[] Results
        {
            get
            {
                var results = new (string, byte[])[filesBytes.Length];
                for (int i = 0; i < filesBytes.Length; i++)
                {
                    results[i] = (fileNames[i], filesBytes[i]);
                }
                return results;
            }
        }

        public (string, byte[]) Current => (fileNames[index], filesBytes[index]);

        object IEnumerator.Current => Current;
    }
}
