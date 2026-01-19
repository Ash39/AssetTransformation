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
    /// <param name="result">The result of a file transformation, including file metadata, transformed data, and any additional data.</param>
    /// <returns>A byte array containing the selected data. Returns an empty array if no matching data is found.</returns>
    public delegate byte[] SelectDelegate(TransformResult result);

    /// <summary>
    /// Provides a collection-like interface for loading, transforming, filtering, and grouping files,
    /// supporting staged transformations and caching within an application-specific directory.
    /// </summary>
    public class FileTransform : IEnumerator<TransformResult>, IEnumerable<TransformResult>
    {
        private readonly TransformResult[] transforms;
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
            ArgumentNullException.ThrowIfNull(files);

            transforms = new TransformResult[files.Length];

            for (int i = 0; i < files.Length; i++)
            {
                transforms[i] = new TransformResult(new FileInfo(files[i]), File.ReadAllBytes(files[i]), string.Empty, string.Empty);
            }

            appRootPath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                appName
            );

            Directory.CreateDirectory(appRootPath);
        }

        private FileTransform(string appRootPath, TransformResult[] transforms)
        {
            this.appRootPath = appRootPath;
            this.transforms = transforms;
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
        /// metadata and an optional string representing the sender or context for the transformation, and returns the transformed byte array. 
        /// Cannot be null.</param>
        /// <returns>A new FileTransform instance containing the transformed files for the specified stage.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stage"/> or <paramref name="func"/> is null.</exception>
        public FileTransform Select(string stage, SelectDelegate func)
        {
            ArgumentNullException.ThrowIfNull(stage);
            ArgumentNullException.ThrowIfNull(func);

            string stageFolder = Path.Combine(appRootPath, stage);
            Directory.CreateDirectory(stageFolder);

            TransformResult[] fileCaches = new TransformResult[transforms.Length];

            List<string> cachedFiles = [.. Directory.GetFiles(stageFolder).Select(f => Path.GetFileName(f))];

            for (int i = 0; i < transforms.Length; i++)
            {
                TransformResult result = transforms[i];

                byte[] hash = ComputeTransformHash(result.Data, result.Info.Name, func);
                string cacheName = Convert.ToHexString(hash) + ".bin";
                string cachePath = Path.Combine(stageFolder, cacheName);
                
                if (!File.Exists(cachePath))
                {
                    byte[] transformed = func(result);
                    File.WriteAllBytes(cachePath, transformed);
                    File.WriteAllText(cachePath + ".json", result.AdditionalInfo);
                    CacheMiss?.Invoke();

                    fileCaches[i] = new(result.Info, transformed, result.AdditionalInfo, cachePath);
                }
                else
                {
                    cachedFiles.Remove(cacheName);
                    cachedFiles.Remove(cacheName + ".json");
                    fileCaches[i] = new(result.Info, File.ReadAllBytes(cachePath), File.ReadAllText(cachePath + ".json"), cachePath);
                }
            }

            foreach (var filePath in cachedFiles)
            {
                File.Delete(filePath);
            }

            return new FileTransform(appRootPath, fileCaches);
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
        /// metadata and an optional string representing the sender or context for the transformation, and returns the transformed byte array. 
        /// Cannot be null. Must be thread-safe.</param>
        /// <returns>A FileTransform instance containing the transformed file data and associated file names.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stage"/> or <paramref name="func"/> is null.</exception>
        public FileTransform SelectMultiThread(string stage, SelectDelegate func)
        {
            ArgumentNullException.ThrowIfNull(stage);
            ArgumentNullException.ThrowIfNull(func);

            string stageFolder = Path.Combine(appRootPath, stage);
            Directory.CreateDirectory(stageFolder);

            TransformResult[] fileCaches = new TransformResult[transforms.Length];

            List<string> cachedFiles = [.. Directory.GetFiles(stageFolder).Select(f => Path.GetFileName(f))];

            Parallel.For( 0, transforms.Length, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },  i =>
            {
                TransformResult result = transforms[i];

                byte[] hash = ComputeTransformHash(result.Data, result.Info.Name, func);
                string cacheName = Convert.ToHexString(hash) + ".bin";
                string cachePath = Path.Combine(stageFolder, cacheName);

                if (!File.Exists(cachePath))
                {
                    byte[] transformed = func(result);
                    File.WriteAllBytes(cachePath, transformed);
                    File.WriteAllText(cachePath + ".json", result.AdditionalInfo);
                    CacheMiss?.Invoke();

                    fileCaches[i] = new(result.Info, transformed, result.AdditionalInfo, cachePath);
                }
                else
                {
                    cachedFiles.Remove(cacheName);
                    cachedFiles.Remove(cacheName + ".json");
                    fileCaches[i] = new(result.Info, File.ReadAllBytes(cachePath), File.ReadAllText(cachePath + ".json"), cachePath);
                }
            });

            foreach (var filePath in cachedFiles)
            {
                File.Delete(filePath);
            }

            return new FileTransform(appRootPath, fileCaches);
        }

        /// <summary>
        /// Filters the current collection of files using the specified predicate and returns a new FileTransform
        /// containing only the files that match the condition.
        /// </summary>
        /// <param name="predicate">A function that determines whether a file should be included in the result. The function receives the file's
        /// byte array, its file metadata and any additional information provided as parameters and returns <see langword="true"/> to include the file; otherwise,
        /// <see langword="false"/>.</param>
        /// <returns>A new FileTransform instance containing only the files for which the predicate returns <see
        /// langword="true"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is <see langword="null"/>.</exception>
        public FileTransform Where(Func<TransformResult, bool> predicate)
        {
            ArgumentNullException.ThrowIfNull(predicate);

            var filteredTransforms = new List<TransformResult>();

            for (int i = 0; i < transforms.Length; i++)
            {
                if (predicate(transforms[i]))
                {
                    filteredTransforms.Add(transforms[i]);
                }
            }

            return new FileTransform(appRootPath, [.. filteredTransforms]);
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
            ArgumentNullException.ThrowIfNull(other);
            if (this.appRootPath != other.appRootPath)
                throw new InvalidOperationException("Cannot combine FileTransforms with different app roots.");

            TransformResult[] combinedtransforms = new TransformResult[this.transforms.Length + other.transforms.Length];

            Array.Copy(this.transforms, 0, combinedtransforms, 0, this.transforms.Length);
            Array.Copy(other.transforms, 0, combinedtransforms, this.transforms.Length, other.transforms.Length);

            return new FileTransform(appRootPath, combinedtransforms);
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
            ArgumentNullException.ThrowIfNull(pattern);

            var regex = new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.Compiled);

            var matchResults = new List<TransformResult>();

            var nonMatchResults = new List<TransformResult>();

            for (int i = 0; i < transforms.Length; i++)
            {
                TransformResult result = transforms[i];

                if (regex.IsMatch(result.Info.Name))
                {
                    matchResults.Add(result);
                }
                else
                {
                    nonMatchResults.Add(result);
                }
            }

            return
            [
                new FileTransform(appRootPath, [.. matchResults]),
                new FileTransform(appRootPath, [.. nonMatchResults])
            ];
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
        public FileTransform SplitBy(Func<TransformResult, string, string> keySelector, params (string, Func<FileTransform, FileTransform> func)[] funcs)
        {
            Dictionary<string, FileTransform> groups = SplitBy(keySelector);

            var tasks = new Task<FileTransform>[funcs.Length];

            for (int i = 0; i < funcs.Length; i++)
            {
                var (key, func) = funcs[i];

                ArgumentNullException.ThrowIfNull(func);

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
        /// <param name="keySelector">A function that takes the file's byte array, additional information and metadata, and returns a string key used to group files.</param>
        /// <returns>A dictionary where each key is a group identifier returned by the selector, and each value is a
        /// FileTransform containing the files in that group.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="keySelector"/> is <see langword="null"/>.</exception>
        public Dictionary<string, FileTransform> SplitBy(Func<TransformResult, string, string> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);

            var groups = new Dictionary<string, List<TransformResult>>();

            for (int i = 0; i < transforms.Length; i++)
            {
                string key = keySelector(transforms[i], transforms[i].Info.Name) ?? string.Empty;

                if (!groups.TryGetValue(key, out var list))
                {
                    list = [];
                    groups[key] = list;
                }

                list.Add(transforms[i]);
            }

            var result = new Dictionary<string, FileTransform>();

            foreach (var kvp in groups)
            {
                result[kvp.Key] = new FileTransform(appRootPath, kvp.Value.ToArray());
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
                return [];

            byte[] il = body.GetILAsByteArray()!;
            return SHA256.HashData(il);
        }

        private static byte[] GetClosureStateHash(Delegate del)
        {
            object? target = del.Target;
            if (target == null)
                return [];

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
            if (transforms.Length > nextIndex)
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
            GC.SuppressFinalize(this);
        }

        /// <inheritdoc/>
        public IEnumerator<TransformResult> GetEnumerator()
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
        /// <remarks>Each element in the array is a <see cref="TransformResult"/>.
        /// The order of elements corresponds to the order in which the files were processed.</remarks>
        public TransformResult[] Results => transforms;

        /// <summary>
        /// Gets the current file name and its associated byte content as a <see cref="TransformResult"/>.
        /// </summary>
        /// <remarks>The returned values reflect the current position within the underlying collection.</remarks>
        public TransformResult Current => transforms[index];

        object IEnumerator.Current => Current;
    }
}
