import bz2
import os

def process_index_file(index_source, persist=True):
    index_offsets_persisted = index_source + ".offsets"

    if os.path.exists(index_offsets_persisted):
        try:
            index_filehandle = open(index_offsets_persisted, "r")
            offset_strings = index_filehandle.readlines()
            sorted_offset_strings = [int(offset) for offset in offset_strings]
            return sorted_offset_strings
        finally:
            index_filehandle.close()

    else:
        stream_offsets = set()
        try:
            index_filehandle = bz2.BZ2File(index_source)

            last_offset = -1
            for line in index_filehandle:
                offset = int(line.decode("utf-8").split(":")[0])
                if offset != last_offset:
                    stream_offsets.add(offset)
                    last_offset = offset
        finally:
            index_filehandle.close()

        sorted_stream_offsets = sorted(stream_offsets)

        if persist:
            try:
                offset_output_filehandle = open(index_offsets_persisted, "w")
                sorter_stream_offset_strings = [str(offset) for offset in sorted_stream_offsets]
                sorter_stream_offset_string = '\n'.join(sorter_stream_offset_strings)

                offset_output_filehandle.write(sorter_stream_offset_string)
            finally:
                offset_output_filehandle.close()

        return sorted_stream_offsets