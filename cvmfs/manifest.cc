/**
 * This file is part of the CernVM File System.
 */

#include "manifest.h"

#include <cstdio>
#include <map>

#include "catalog.h"
#include "util.h"

using namespace std;  // NOLINT

namespace manifest {

static void ParseKeyvalMem(const unsigned char *buffer,
                           const unsigned buffer_size,
                           map<char, string> *content)
{
  string line;
  unsigned pos = 0;
  while (pos < buffer_size) {
    if (static_cast<char>(buffer[pos]) == '\n') {
      if (line == "--")
        return;

      if (line != "") {
        const string tail = (line.length() == 1) ? "" : line.substr(1);
        // Special handling of 'Z' key because it can exist multiple times
        if (line[0] != 'Z') {
          (*content)[line[0]] = tail;
        } else {
          if (content->find(line[0]) == content->end()) {
            (*content)[line[0]] = tail;
          } else {
            (*content)[line[0]] = (*content)[line[0]] + "|" + tail;
          }
        }
      }
      line = "";
    } else {
      line += static_cast<char>(buffer[pos]);
    }
    pos++;
  }
}


static bool ParseKeyvalPath(const string &filename,
                            map<char, string> *content)
{
  int fd = open(filename.c_str(), O_RDONLY);
  if (fd < 0)
    return false;

  unsigned char buffer[4096];
  int num_bytes = read(fd, buffer, sizeof(buffer));
  close(fd);

  if ((num_bytes <= 0) || (unsigned(num_bytes) >= sizeof(buffer)))
    return false;

  ParseKeyvalMem(buffer, unsigned(num_bytes), content);
  return true;
}


Manifest *Manifest::LoadMem(const unsigned char *buffer,
                            const unsigned length)
{
  map<char, string> content;
  ParseKeyvalMem(buffer, length, &content);

  return Load(content);
}


Manifest *Manifest::LoadFile(const std::string &from_path) {
  map<char, string> content;
  if (!ParseKeyvalPath(from_path, &content))
    return NULL;

  return Load(content);
}


Manifest *Manifest::Load(const map<char, string> &content) {
  map<char, string>::const_iterator iter;

  // Required keys
  shash::Any catalog_hash;
  shash::Md5 root_path;
  uint32_t ttl;
  uint64_t revision;

  iter = content.find('C');
  if ((iter = content.find('C')) == content.end())
    return NULL;
  catalog_hash = MkFromHexPtr(shash::HexPtr(iter->second));
  if ((iter = content.find('R')) == content.end())
    return NULL;
  root_path = shash::Md5(shash::HexPtr(iter->second));
  if ((iter = content.find('D')) == content.end())
    return NULL;
  ttl = String2Uint64(iter->second);
  if ((iter = content.find('S')) == content.end())
    return NULL;
  revision = String2Uint64(iter->second);

  // Optional keys
  shash::Any micro_catalog_hash;
  string repository_name;
  shash::Any certificate;
  shash::Any history;
  uint64_t publish_timestamp = 0;

  if ((iter = content.find('L')) != content.end())
    micro_catalog_hash = MkFromHexPtr(shash::HexPtr(iter->second));
  if ((iter = content.find('N')) != content.end())
    repository_name = iter->second;
  if ((iter = content.find('X')) != content.end())
    certificate = MkFromHexPtr(shash::HexPtr(iter->second));
  if ((iter = content.find('H')) != content.end())
    history = MkFromHexPtr(shash::HexPtr(iter->second));
  if ((iter = content.find('T')) != content.end())
    publish_timestamp = String2Uint64(iter->second);

  // Z expands to a pipe-separated string of channel-hash pairs
  vector<history::TagList::ChannelTag> channel_tops;
  if ((iter = content.find('Z')) != content.end()) {
    vector<string> elements = SplitString(iter->second, '|');
    for (unsigned i = 0; i < elements.size(); ++i) {
      assert(elements[i].length() > 2);
      int channel_int = 16 * HexDigit2Int(elements[i][0]) +
                        HexDigit2Int(elements[i][1]);
      history::UpdateChannel channel =
        static_cast<history::UpdateChannel>(channel_int);
      channel_tops.push_back(history::TagList::ChannelTag(
        channel, MkFromHexPtr(shash::HexPtr(elements[i].substr(2)))));
    }
  }

  return new Manifest(catalog_hash, root_path, ttl, revision,
                      micro_catalog_hash, repository_name, certificate,
                      history, publish_timestamp, channel_tops);
}


Manifest::Manifest(const shash::Any &catalog_hash, const string &root_path) {
  catalog_hash_ = catalog_hash;
  root_path_ = shash::Md5(shash::AsciiPtr(root_path));
  ttl_ = catalog::Catalog::kDefaultTTL;
  revision_ = 0;
  publish_timestamp_ = 0;
}


/**
 * Creates the manifest string
 */
string Manifest::ExportString() const {
  string manifest =
    "C" + catalog_hash_.ToString() + "\n" +
    "R" + root_path_.ToString() + "\n" +
    "D" + StringifyInt(ttl_) + "\n" +
    "S" + StringifyInt(revision_) + "\n";

  if (!micro_catalog_hash_.IsNull())
    manifest += "L" + micro_catalog_hash_.ToString() + "\n";
  if (repository_name_ != "")
    manifest += "N" + repository_name_ + "\n";
  if (!certificate_.IsNull())
    manifest += "X" + certificate_.ToString() + "\n";
  if (!history_.IsNull())
    manifest += "H" + history_.ToString() + "\n";
  if (publish_timestamp_ > 0)
    manifest += "T" + StringifyInt(publish_timestamp_) + "\n";

  for (unsigned i = 0; i < channel_tops_.size(); ++i) {
    manifest += "Z" + StringifyByteAsHex(channel_tops_[i].channel) +
                channel_tops_[i].root_hash.ToString() + "\n";
  }

  return manifest;
}



/**
 * Writes the .cvmfspublished file (unsigned).
 */
bool Manifest::Export(const std::string &path) const {
  FILE *fmanifest = fopen(path.c_str(), "w");
  if (!fmanifest)
    return false;

  string manifest = ExportString();

  if (fwrite(manifest.data(), 1, manifest.length(), fmanifest) !=
      manifest.length())
  {
    fclose(fmanifest);
    unlink(path.c_str());
    return false;
  }
  fclose(fmanifest);

  return true;
}


/**
 * Writes the cvmfschecksum.$repository file.  Atomic store.
 */
bool Manifest::ExportChecksum(const string &directory, const int mode) const {
  string checksum_path = MakeCanonicalPath(directory) + "/cvmfschecksum." +
                         repository_name_;
  string checksum_tmp_path;
  FILE *fchksum = CreateTempFile(checksum_path, mode, "w", &checksum_tmp_path);
  if (fchksum == NULL)
    return false;
  string cache_checksum = catalog_hash_.ToString() + "T" +
                          StringifyInt(publish_timestamp_);
  int written = fwrite(&(cache_checksum[0]), 1, cache_checksum.length(),
                       fchksum);
  fclose(fchksum);
  if (static_cast<unsigned>(written) != cache_checksum.length()) {
    unlink(checksum_tmp_path.c_str());
    return false;
  }
  int retval = rename(checksum_tmp_path.c_str(), checksum_path.c_str());
  if (retval != 0) {
    unlink(checksum_tmp_path.c_str());
    return false;
  }
  return true;
}

}  // namespace manifest
