using System;
using System.Collections.Generic;
using System.Text;

namespace AssetTransformation
{
    /// <summary>
    /// Represents the result of a file transformation, including file metadata, transformed data, and any additional
    /// information.
    /// </summary>
    /// <param name="info">The metadata of the file that was transformed. Provides access to file properties such as name, path, and size.</param>
    /// <param name="data">The transformed data as a byte array. Contains the output produced by the transformation process.</param>
    /// <param name="additionalInfo">Additional information related to the transformation. May include context, status messages, or other relevant
    /// details.</param>
    public class TransformResult(FileInfo info, byte[] data, string additionalInfo)
    {
        /// <summary>
        /// Gets the metadata of the file that was transformed.
        /// </summary>
        public FileInfo Info { get; internal set; } = info;
        /// <summary>
        /// Gets the transformed data as a byte array.
        /// </summary>
        public byte[] Data { get; internal set; } = data;
        /// <summary>
        /// Gets additional information related to the transformation.
        /// </summary>
        public string AdditionalInfo { get; set; } = additionalInfo;
    }
}
