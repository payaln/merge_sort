#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <map>
#include <thread>
#include <mutex>


#define INPUT_FILE "input5"
#define OUTPUT_FILE "output"
#define BUFFER_SIZE 16000
#define NUM_SIZE sizeof(unsigned int)

unsigned int primary_sort(){

    std::mutex locker;

    unsigned int file_no = 0;

    std::thread th1([&locker, &file_no]{
        std::ifstream in(INPUT_FILE, std::ios::binary | std::ios::ate);
        unsigned int in_size = static_cast<unsigned int>(in.tellg());
        in.seekg(0);
        std::vector<unsigned int> buf(BUFFER_SIZE);
        while (!in.eof() && (in.tellg() >= 0 && in.tellg() < in_size)){
            in.read((char *)&buf[0], BUFFER_SIZE * NUM_SIZE);
            if (in.gcount()){
                buf.resize(in.gcount() / NUM_SIZE);
                std::sort(buf.begin(), buf.end());
                locker.lock();
                std::ofstream out("tmp" + std::to_string(file_no), std::ios::binary);
                ++file_no;
                locker.unlock();
                out.write((char *)&buf[0], in.gcount());
                in.seekg(BUFFER_SIZE * NUM_SIZE, std::ios::cur);
                out.close();
            }
        }
        in.close();
    });

    std::thread th2([&locker, &file_no]{
        std::ifstream in(INPUT_FILE, std::ios::binary | std::ios::ate);
        unsigned int in_size = static_cast<unsigned int>(in.tellg());
        in.seekg(BUFFER_SIZE * NUM_SIZE);
        std::vector<unsigned int> buf(BUFFER_SIZE);
        while (!in.eof() && (in.tellg() >= 0 && in.tellg() < in_size)){
            in.read((char *)&buf[0], BUFFER_SIZE * NUM_SIZE);
            if (in.gcount()){
                buf.resize(in.gcount() / NUM_SIZE);
                std::sort(buf.begin(), buf.end());
                locker.lock();
                std::ofstream out("tmp" + std::to_string(file_no), std::ios::binary);
                ++file_no;
                locker.unlock();
                out.write((char *)&buf[0], in.gcount());
                in.seekg(BUFFER_SIZE * NUM_SIZE, std::ios::cur);
                out.close();
            }
        }
        in.close();
    });

    if (th1.joinable())
        th1.join();
    if (th2.joinable())
        th2.join();

    return file_no;
}

void merge(unsigned int count_files){

    unsigned int block_size = 2 * BUFFER_SIZE / count_files;

    std::multimap<unsigned int, unsigned int> sorted_list;
    std::vector<std::vector<unsigned int>> buffers(count_files, std::vector<unsigned int>(block_size));
    std::vector<unsigned int> buf_ofset(count_files, 0);
    std::vector<std::ifstream> in_files(count_files);

    for (unsigned int i = 0; i < count_files; ++i) {
        in_files[i].open("tmp" + std::to_string(i), std::ios::binary);
        in_files[i].read((char *)&buffers[i][0], block_size * NUM_SIZE);
        if (in_files[i].gcount()) {
            buffers[i].resize(in_files[i].gcount() / NUM_SIZE);
            sorted_list.insert({buffers[i][buf_ofset[i]++], i});
        }
    }

    std::ofstream out(OUTPUT_FILE, std::ios::binary);

    while (sorted_list.size() > 1){

        out.write((char *)&sorted_list.begin()->first, NUM_SIZE);

        unsigned int i = sorted_list.begin()->second;
        sorted_list.erase(sorted_list.begin());

        if (buffers[i].size() == buf_ofset[i]){
            if (!in_files[i].eof()){
                in_files[i].read((char *) &buffers[i][0], block_size * NUM_SIZE);
                buf_ofset[i] = 0;
                if (in_files[i].gcount()) {
                    buffers[i].resize(in_files[i].gcount() / NUM_SIZE);
                    sorted_list.insert({buffers[i][buf_ofset[i]++], i});
                }
            }
        } else {
            sorted_list.insert({buffers[i][buf_ofset[i]++], i});
        }

    }

    unsigned int j = sorted_list.begin()->second;
    out.write((char *)&buffers[j][buf_ofset[j] - 1], (buffers[j].size() - buf_ofset[j] + 1) * NUM_SIZE);

    while (!in_files[j].eof()){
        in_files[j].read((char *) &buffers[j][0], block_size * NUM_SIZE);
        if (in_files[j].gcount()) {
            out.write((char *)&buffers[j][0], in_files[j].gcount());
        }
    }

    for (unsigned int i  = 0; i < count_files; ++i){
        in_files[i].close();
        std::string file_name = "tmp" + std::to_string(i);
        std::remove(file_name.c_str());
    }

    out.close();
}

int main() {

    merge(primary_sort());

    return 0;
}