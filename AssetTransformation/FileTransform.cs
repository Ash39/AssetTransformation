using System.Collections;
using System.Collections.Concurrent;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace AssetTransformation
{
    /// <summary>
    /// Represents a method that selects and returns a byte array based on the provided source data and name, optionally
    /// modifying the sender object.
    /// </summary>
    /// <param name="source">The source byte array from which data is selected. Cannot be null.</param>
    /// <param name="name">The name or key used to identify the data to select from the source. Cannot be null or empty.</param>
    /// <param name="sender">A reference to an object that may be modified or used during selection. The method may update this object to
    /// provide additional context or results.</param>
    /// <returns>A byte array containing the selected data. Returns an empty array if no matching data is found.</returns>
    public delegate byte[] SelectDelegate(byte[] source, FileInfo info,ref object sender);

    /// <summary>
    /// Provides a collection-like interface for loading, transforming, filtering, and grouping files,
    /// supporting staged transformations and caching within an application-specific directory.
    /// </summary>
    public class FileTransform : IEnumerator<(FileInfo info, byte[] data, object sender)>, IEnumerable<(FileInfo info, byte[] data, object sender)>
    {
        private readonly byte[][] filesBytes;
        private readonly FileInfo[] fileInfos;
        private readonly object[] senders;
        private readonly string appRootPath;
        private int index = -1;

        internal Action? CacheMiss;

        /// <summary>
        /// Initializes a new instance of the FileTransform class, loading the specified files and preparing the
        /// application data directory for file operations.
        /// </summary>
        /// <remarks>The constructor creates the application data directory if it does not already exist.
        /// Each file in the <paramref name="files"/> array is read immediately; ensure that all specified files exist
        /// and are accessible.</remarks>
        /// <param name="appName">The name of the application. Used to determine the root path for storing application-specific data.</param>
        /// <param name="files">An array of file paths to be loaded. Each file is read and its contents are stored for later use.</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="files"/> is <see langword="null"/>.</exception>
        public FileTransform(string appName, string[] files)
        {
            if (files == null) throw new ArgumentNullException(nameof(files));

            filesBytes = new byte[files.Length][];
            fileInfos = new FileInfo[files.Length];
            senders = new object[files.Length];

            for (int i = 0; i < files.Length; i++)
            {
                filesBytes[i] = File.ReadAllBytes(files[i]);
                fileInfos[i] = new FileInfo(files[i]);
            }

            appRootPath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                appName
            );

            Directory.CreateDirectory(appRootPath);
        }

        private FileTransform(string appRootPath, byte[][] filesBytes, FileInfo[] fileInfos, object[] senders)
        {
            this.appRootPath = appRootPath;
            this.filesBytes = filesBytes;
            this.fileInfos = fileInfos;
            this.senders = senders;
        }

        /// <summary>
        /// Applies a transformation function to each file in the current set, caching the results under the specified
        /// stage, and returns a new FileTransform containing the transformed files.
        /// </summary>
        /// <remarks>If a transformation result for a file and stage already exists in the cache, it is
        /// reused; otherwise, the transformation is computed and cached. This improves performance when applying the
        /// same transformation multiple times.</remarks>
        /// <param name="stage">The name of the transformation stage. Used to organize cached results for this stage. Cannot be null.</param>
        /// <param name="func">A function that transforms the contents of each file. The function receives the file's byte array, its
        /// name and an optional object representing the sender or context for the transformation, and returns the transformed byte array. 
        /// Cannot be null.</param>
        /// <returns>A new FileTransform instance containing the transformed files for the specified stage.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stage"/> or <paramref name="func"/> is null.</exception>
        public FileTransform Select(string stage, SelectDelegate func)
        {
            if (stage == null) throw new ArgumentNullException(nameof(stage));
            if (func == null) throw new ArgumentNullException(nameof(func));

            string stageFolder = Path.Combine(appRootPath, stage);
            Directory.CreateDirectory(stageFolder);

            byte[][] fileCaches = new byte[filesBytes.Length][];

            for (int i = 0; i < filesBytes.Length; i++)
            {
                byte[] inputBytes = filesBytes[i];
                FileInfo fileInfo = fileInfos[i];

                byte[] hash = ComputeTransformHash(inputBytes, fileInfo.Name, func);
                string cacheName = Convert.ToHexString(hash) + ".bin";
                string cachePath = Path.Combine(stageFolder, cacheName);

                if (!File.Exists(cachePath))
                {
                    byte[] transformed = func(inputBytes, fileInfo, ref senders[i]);
                    File.WriteAllBytes(cachePath, transformed);
                    CacheMiss?.Invoke();
                }

                fileCaches[i] = File.ReadAllBytes(cachePath);
            }

            return new FileTransform(appRootPath, fileCaches, fileInfos, senders);
        }

        /// <summary>
        /// Applies a transformation function to each file in parallel and returns a new FileTransform containing the
        /// transformed file data.
        /// </summary>
        /// <remarks>Transformed files are cached in a stage-specific folder using a hash of their content
        /// and name. If a cache file already exists, it is reused. The transformation is performed in parallel,
        /// utilizing all available processor cores.</remarks>
        /// <param name="stage">The name of the processing stage. Used to determine the output folder for cached transformed files. Cannot
        /// be null.</param>
        /// <param name="func">A function that transforms the contents of each file. The function receives the file's byte array, its
        /// name and an optional object representing the sender or context for the transformation, and returns the transformed byte array. 
        /// Cannot be null. Must be thread-safe.</param>
        /// <returns>A FileTransform instance containing the transformed file data and associated file names.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stage"/> or <paramref name="func"/> is null.</exception>
        public FileTransform SelectMultiThread(string stage, SelectDelegate func)
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
                    FileInfo fileInfo = fileInfos[i];

                    byte[] hash = ComputeTransformHash(inputBytes, fileInfo.Name, func);
                    string cacheName = Convert.ToHexString(hash) + ".bin";
                    string cachePath = Path.Combine(stageFolder, cacheName);

                    if (!File.Exists(cachePath))
                    {
                        byte[] transformed = func(inputBytes, fileInfo, ref senders[i]);
                        File.WriteAllBytes(cachePath, transformed);
                        CacheMiss?.Invoke();
                    }

                    fileCaches[i] = File.ReadAllBytes(cachePath);
                });

            return new FileTransform(appRootPath, fileCaches, fileInfos, senders);
        }

        /// <summary>
        /// Filters the current collection of files using the specified predicate and returns a new FileTransform
        /// containing only the files that match the condition.
        /// </summary>
        /// <param name="predicate">A function that determines whether a file should be included in the result. The function receives the file's
        /// byte array and its name as parameters and returns <see langword="true"/> to include the file; otherwise,
        /// <see langword="false"/>.</param>
        /// <returns>A new FileTransform instance containing only the files for which the predicate returns <see
        /// langword="true"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public FileTransform Where(Func<byte[], FileInfo, bool> predicate)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));

            var filteredBytes = new List<byte[]>();
            var filteredFileInfos = new List<FileInfo>();
            var filteredSenders = new List<object>();

            for (int i = 0; i < filesBytes.Length; i++)
            {
                if (predicate(filesBytes[i], fileInfos[i]))
                {
                    filteredBytes.Add(filesBytes[i]);
                    filteredFileInfos.Add(fileInfos[i]);
                    filteredSenders.Add(senders[i]);
                }
            }

            return new FileTransform(appRootPath, [.. filteredBytes], [.. filteredFileInfos], [.. filteredSenders]);
        }

        /// <summary>
        /// Combines the current FileTransform with another FileTransform that shares the same application root path.
        /// </summary>
        /// <remarks>The combined FileTransform will include all files and names from both instances,
        /// preserving their original order. This method does not modify either of the original FileTransform
        /// instances.</remarks>
        /// <param name="other">The FileTransform to combine with the current instance. Must not be null and must have the same application
        /// root path.</param>
        /// <returns>A new FileTransform containing the files and names from both the current and specified FileTransform
        /// instances.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="other"/> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown if <paramref name="other"/> has a different application root path than the current instance.</exception>
        public FileTransform Concat(FileTransform other)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            if (this.appRootPath != other.appRootPath)
                throw new InvalidOperationException("Cannot combine FileTransforms with different app roots.");

            byte[][] combinedBytes = new byte[this.filesBytes.Length + other.filesBytes.Length][];
            FileInfo[] combinedFileInfos = new FileInfo[this.fileInfos.Length + other.fileInfos.Length];
            object[] combinedSenders = new object[this.senders.Length + other.senders.Length];

            Array.Copy(this.filesBytes, 0, combinedBytes, 0, this.filesBytes.Length);
            Array.Copy(other.filesBytes, 0, combinedBytes, this.filesBytes.Length, other.filesBytes.Length);

            Array.Copy(this.fileInfos, 0, combinedFileInfos, 0, this.fileInfos.Length);
            Array.Copy(other.fileInfos, 0, combinedFileInfos, this.fileInfos.Length, other.fileInfos.Length);

            Array.Copy(this.senders, 0, combinedSenders, 0, this.senders.Length);
            Array.Copy(other.senders, 0, combinedSenders, this.senders.Length, other.senders.Length);

            return new FileTransform(appRootPath, combinedBytes, combinedFileInfos, combinedSenders);
        }

        /// <summary>
        /// Splits the current set of files into two groups based on whether their names match the specified regular
        /// expression pattern.
        /// </summary>
        /// <remarks>The method uses case-insensitive matching for the provided pattern. The order of
        /// files in each group is preserved from the original set.</remarks>
        /// <param name="pattern">A regular expression pattern used to determine which file names are included in the first group. Matching is
        /// case-insensitive.</param>
        /// <returns>An array containing two <see cref="FileTransform"/> instances: the first for files whose names match the
        /// pattern, and the second for files that do not match. Each instance contains the corresponding file names and
        /// their associated data.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="pattern"/> is <see langword="null"/>.</exception>
        public FileTransform[] Split(string pattern)
        {
            if (pattern == null) throw new ArgumentNullException(nameof(pattern));

            var regex = new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.Compiled);

            var matchBytes = new List<byte[]>();
            var matchFileInfos = new List<FileInfo>();
            var matchSenders = new List<object>();

            var nonMatchBytes = new List<byte[]>();
            var nonMatchFileInfos = new List<FileInfo>();
            var nonMatchSenders = new List<object>();

            for (int i = 0; i < filesBytes.Length; i++)
            {
                FileInfo info = fileInfos[i];
                byte[] bytes = filesBytes[i];
                object sender = senders[i];

                if (regex.IsMatch(info.Name))
                {
                    matchBytes.Add(bytes);
                    matchFileInfos.Add(info);
                    matchSenders.Add(sender);
                }
                else
                {
                    nonMatchBytes.Add(bytes);
                    nonMatchFileInfos.Add(info);
                    nonMatchSenders.Add(sender);
                }
            }

            return new[]
            {
                new FileTransform(appRootPath, [.. matchBytes], [.. matchFileInfos], [.. matchSenders]),
                new FileTransform(appRootPath, [.. nonMatchBytes], [.. nonMatchFileInfos], [.. nonMatchSenders])
            };
        }

        /// <summary>
        /// Splits the current file transform into two parts based on the specified pattern and applies
        /// separate transformations to each part.
        /// </summary>
        /// <remarks>
        /// The <paramref name="match"/> and <paramref name="nonMatch"/> functions are executed in parallel.
        /// The order of concatenation is preserved: the result of <paramref name="match"/> is followed by
        /// the result of <paramref name="nonMatch"/>. If either function throws an exception, it is propagated.
        /// </remarks>
        /// <param name="pattern">The pattern used to divide the current file transform into matching and non-matching segments.</param>
        /// <param name="match">A transformation applied to the matching segment.</param>
        /// <param name="nonMatch">A transformation applied to the non-matching segment.</param>
        /// <returns>
        /// A new <see cref="FileTransform"/> representing the concatenation of the transformed segments.
        /// </returns>
        public FileTransform Split(string pattern, Func<FileTransform, FileTransform> match, Func<FileTransform, FileTransform> nonMatch)
        {
            FileTransform[] splits = Split(pattern);

            Task<FileTransform> matchTask = Task.Run(() => match(splits[0]));
            Task<FileTransform> nonMatchTask = Task.Run(() => nonMatch(splits[1]));

            Task.WaitAll(matchTask, nonMatchTask);

            return matchTask.Result.Concat(nonMatchTask.Result);
        }

        /// <summary>
        /// Splits the current file transform into groups using the specified key selector, applies
        /// the provided transformations to each group in parallel, and concatenates the results.
        /// </summary>
        /// <remarks>
        /// Transformations are executed in parallel. The concatenation order of the results corresponds
        /// to the order of the <paramref name="funcs"/> array.
        /// </remarks>
        /// <param name="keySelector">
        /// A function that selects a grouping key for each file segment based on its byte content
        /// and associated identifier.
        /// </param>
        /// <param name="funcs">
        /// An array of tuples mapping a group key to a transformation function.
        /// </param>
        /// <returns>
        /// A <see cref="FileTransform"/> representing the concatenation of the transformed groups.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// Thrown if any transformation function is null.
        /// </exception>
        /// <exception cref="KeyNotFoundException">
        /// Thrown if a specified group key does not exist.
        /// </exception>
        public FileTransform SplitBy(Func<byte[], string, string> keySelector, params (string key, Func<FileTransform, FileTransform> func)[] funcs)
        {
            Dictionary<string, FileTransform> groups = SplitBy(keySelector);

            var tasks = new Task<FileTransform>[funcs.Length];

            for (int i = 0; i < funcs.Length; i++)
            {
                var (key, func) = funcs[i];

                if (func == null)
                    throw new ArgumentNullException(nameof(func));

                if (!groups.TryGetValue(key, out var transform))
                    throw new KeyNotFoundException($"No group found for key '{key}'.");

                tasks[i] = Task.Run(() => func(transform));
            }

            Task.WaitAll(tasks);

            FileTransform result = tasks[0].Result;

            for (int i = 1; i < tasks.Length; i++)
            {
                result = result.Concat(tasks[i].Result);
            }

            return result;
        }

        /// <summary>
        /// Splits the current collection of files into groups based on a key generated by the specified selector
        /// function.
        /// </summary>
        /// <remarks>If the selector returns <see langword="null"/>, the file is grouped under an empty
        /// string key. Each group contains a new FileTransform instance with the files that share the same
        /// key.</remarks>
        /// <param name="keySelector">A function that takes the file's byte array and name, and returns a string key used to group files.</param>
        /// <returns>A dictionary where each key is a group identifier returned by the selector, and each value is a
        /// FileTransform containing the files in that group.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="keySelector"/> is <see langword="null"/>.</exception>
        public Dictionary<string, FileTransform> SplitBy(Func<byte[], string, string> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));

            var groups = new Dictionary<string, List<(byte[] Bytes, FileInfo Info, object Sender)>>();

            for (int i = 0; i < filesBytes.Length; i++)
            {
                string key = keySelector(filesBytes[i], fileInfos[i].Name) ?? string.Empty;

                if (!groups.TryGetValue(key, out var list))
                {
                    list = new List<(byte[] Bytes, FileInfo Info, object Sender)>();
                    groups[key] = list;
                }

                list.Add((filesBytes[i], fileInfos[i], senders[i]));
            }

            var result = new Dictionary<string, FileTransform>();

            foreach (var kvp in groups)
            {
                var bytes = kvp.Value.Select(v => v.Bytes).ToArray();
                var infos = kvp.Value.Select(v => v.Info).ToArray();
                var senders = kvp.Value.Select(v => v.Sender).ToArray();

                result[kvp.Key] = new FileTransform(appRootPath, bytes, infos, senders);
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

        /// <summary>
        /// Advances the enumerator to the next element in the collection.
        /// </summary>
        /// <returns>true if the enumerator was successfully advanced to the next element; otherwise, false.</returns>
        public bool MoveNext()
        {
            int currentIndex = index;
            int nextIndex = currentIndex + 1;
            if (filesBytes.Length > nextIndex)
            {
                index = nextIndex;
                return true;
            }
            return false;
        }

        /// <summary>
        /// Resets the internal position to the beginning.
        /// </summary>
        /// <remarks>Call this method to restart iteration or processing from the initial position. This
        /// is typically used when reusing the instance for a new operation.</remarks>
        public void Reset()
        {
            index = -1;
        }

        /// <summary>
        /// Releases resources used by the instance and resets its state for potential reuse.
        /// </summary>
        public void Dispose()
        {
            index = -1;
        }

        /// <inheritdoc/>
        public IEnumerator<(FileInfo, byte[], object)> GetEnumerator()
        {
            return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Gets an array containing the names and contents of all processed files.
        /// </summary>
        /// <remarks>Each element in the array is a tuple where the first item is the file name and the
        /// second item is the file's content as a byte array. The order of elements corresponds to the order in which
        /// the files were processed.</remarks>
        public (FileInfo info, byte[] data, object sender)[] Results
        {
            get
            {
                var results = new (FileInfo info, byte[] data, object sender)[filesBytes.Length];
                for (int i = 0; i < filesBytes.Length; i++)
                {
                    results[i] = (fileInfos[i], filesBytes[i], senders[i]);
                }
                return results;
            }
        }

        /// <summary>
        /// Gets the current file name and its associated byte content as a tuple.
        /// </summary>
        /// <remarks>The first item of the tuple is the file name as a string; the second item is the
        /// file's content as a byte array. The returned values reflect the current position within the underlying
        /// collection.</remarks>
        public (FileInfo info, byte[] data, object sender) Current => (fileInfos[index], filesBytes[index], senders[index]);

        object IEnumerator.Current => Current;
    }
}
