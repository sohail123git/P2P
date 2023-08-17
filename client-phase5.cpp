#include <iostream>
#include <string.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <dirent.h>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <unistd.h>
#include <chrono>
#include <cstdlib>
#include <thread>
#include<bits/stdc++.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <openssl/md5.h>
#include <iomanip>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/mman.h>

#define BUFFSIZE 10000
#define MAX_BUFFER_SIZE 10000000

using namespace std;

template <typename T>
void print_array(T* array, int size){
    for(int i=0;i<size;i++){
        cout<<array[i];
        if(i!=size-1)
            cout<<", ";
    }
    cout<<endl;
};

bool contains(vector<string> vector, string str){
    for(int i=0;i<vector.size();i++){
        if(vector[i]==str){
            return true;
        }
    }
    return false;
};

string convert_to_string(char* a, int len)
{
    string s = "";
    for (int i = 0; i < len; i++) {
        if(a[i]!='\0')
            s = s + a[i];
        else
            break;
    }
    return s;
};

string get_reply_for_search(string msg, vector<string> files_i_have){
    string output_str="";
    while(msg.find_first_of("$")!=-1){
        int len=msg.length();
        msg = msg.substr(msg.find_first_of("$")+1);
        len--;
        int len_file_name = min(msg.find_first_of("#"),msg.find_first_of("$"));
        
        string file_name = msg.substr(0,len_file_name);
        msg = msg.substr(len_file_name);


        if (contains(files_i_have, file_name)){
            output_str.append("#1");
        }
        else
            output_str.append("#0");
    }
    return output_str;
};

string recieve(int FD){
    char buff[BUFFSIZE];
    char  *bufptr = buff;
    int bytes_recieved=0;

    while(bytes_recieved<BUFFSIZE){
        int x=recv(FD, bufptr, BUFFSIZE-bytes_recieved, 0);
        if(x==-1)
            perror("recv recieve()");
        else
            bytes_recieved +=x;
            bufptr += x;
    }
    string msg = convert_to_string(buff, BUFFSIZE);
    return msg;
};

void set_up_connections(int listenerFD, int no_peers, int *as_clientFD, int *as_serverFD, vector<int> port_peers){
    //BEGIN util vars
    int sin_size = sizeof(struct sockaddr_in);
    struct timeval timeout;
    timeout.tv_sec=0; 
    timeout.tv_usec=0;
    //END util vars
    int no_TO_conns=0, no_FROM_conns=0;
    bool did_peer_hear[no_peers];
    for(int i=0;i<no_peers;i++)
        did_peer_hear[i]=false;
    
    while(true){
        
        // connection req
        for(int i=0;i<no_peers;i++){
            struct sockaddr_in dest_addr;
            dest_addr.sin_family = AF_INET;
            dest_addr.sin_port = htons(port_peers[i]); 
            inet_aton("127.0.0.1",&(dest_addr.sin_addr));
            memset(&(dest_addr.sin_zero),'\0',8);
            if(did_peer_hear[i] == false){
                
                if(connect(as_clientFD[i], (struct sockaddr *)&dest_addr, sizeof(struct sockaddr)) == 0){
                    did_peer_hear[i]=true;
                    no_FROM_conns++;
                }
                // else
                    // perror("connect 1");
            }
        }
        
        // connection accept
        fd_set listenerFDS;
        FD_SET(listenerFD,&listenerFDS);
        while(true){
            timeout.tv_sec=1; 
            if(select(listenerFD+1,&listenerFDS,NULL,NULL,&timeout)!=-1){
                if(FD_ISSET(listenerFD,&listenerFDS)){
                    struct sockaddr_in their_addr;
                    as_serverFD[no_TO_conns] = accept(listenerFD, (struct sockaddr *)&their_addr, (socklen_t*)&sin_size);
                    no_TO_conns++;
                }
                else
                    break;
            }
            else
                perror("select 1");
        }

        if(no_TO_conns==no_peers && no_FROM_conns==no_peers){
            break;
        }
    }
    return;
};

void set_up_connections(int listenerFD, int no_to_conns, int no_from_conns, int *as_clientFD, int *as_serverFD, vector<int> port_peers){
    //BEGIN util vars
    int sin_size = sizeof(struct sockaddr_in);
    struct timeval timeout;
    timeout.tv_sec=0; 
    timeout.tv_usec=0;
    //END util vars
    int no_TO_conns=0, no_FROM_conns=0;
    bool did_peer_hear[no_to_conns];
    for(int i=0;i<no_to_conns;i++)
        did_peer_hear[i]=false;
    
    while(true){
        
        // connection req
        for(int i=0;i<no_to_conns;i++){
            struct sockaddr_in dest_addr;
            dest_addr.sin_family = AF_INET;
            dest_addr.sin_port = htons(port_peers[i]); 
            inet_aton("127.0.0.1",&(dest_addr.sin_addr));
            memset(&(dest_addr.sin_zero),'\0',8);
            if(did_peer_hear[i] == false){
                
                if(connect(as_clientFD[i], (struct sockaddr *)&dest_addr, sizeof(struct sockaddr)) == 0){
                    did_peer_hear[i]=true;
                    no_TO_conns++;
                    // cout<<"no_TO_conns:"<<no_TO_conns<<endl;
                }
                // else
                    // perror("connect 1");
            }
        }
        
        // connection accept
        fd_set listenerFDS;
        FD_ZERO(&listenerFDS);
        FD_SET(listenerFD,&listenerFDS);
        while(true){
            timeout.tv_sec=1; 
            if(select(listenerFD+1,&listenerFDS,NULL,NULL,&timeout)!=-1){
                if(FD_ISSET(listenerFD,&listenerFDS)){
                    struct sockaddr_in their_addr;
                    as_serverFD[no_FROM_conns] = accept(listenerFD, (struct sockaddr *)&their_addr, (socklen_t*)&sin_size);
                    no_FROM_conns++;
                    // cout<<"no_FROM_conns:"<<no_FROM_conns<<endl;

                }
                else
                    break;
            }
            else
                perror("select 1");
        }

        if(no_TO_conns==no_to_conns && no_FROM_conns==no_from_conns){
            break;
        }
    }
    return;
};

void send_to_peers(int* FDS, int no_peers, string s_msg){
    char msg[BUFFSIZE];
    strcpy(msg,s_msg.c_str());
    for(int i=0;i<no_peers;i++){
        int done = 0;
        while(done<BUFFSIZE){
            int x=send(FDS[i],msg+done,BUFFSIZE-done,0);
            if(x==-1)
                perror("send 1");
            else{
                done+=x;
            }
        }
    }
};

void send_to_peers(int* FDS, int no_peers, string *s_msgs){
    char msg[BUFFSIZE];
    for(int i=0;i<no_peers;i++){
        strcpy(msg, s_msgs[i].c_str());
        int done = 0;
        while(done<BUFFSIZE){
            int x=send(FDS[i],msg+done,BUFFSIZE-done,0);
            if(x==-1)
                perror("send 1");
            else{
                done+=x;
            }
        }
    }
};

void recieve_from_peers(int* FDS, int no_peers, string* msgs){
    struct timeval timeout;
    timeout.tv_sec=0; 
    timeout.tv_usec=0;

    int max_fd = 0;
    for(int i=0;i<no_peers;i++){
        if(FDS[i]>max_fd)
            max_fd = FDS[i];
    }

    fd_set to_be_checked;
    fd_set read_fds;

    FD_ZERO(&to_be_checked);
    FD_ZERO(&read_fds);

    for(int i=0;i<no_peers;i++){
        FD_SET(FDS[i],&to_be_checked);
    }

    int no_msgs_got=0;
    while(true){
        read_fds=to_be_checked;
        timeout.tv_sec=2;
        if(select(max_fd+1, &read_fds, NULL, NULL, &timeout)!=-1){
            for(int i=0;i<no_peers;i++){
                if(FD_ISSET(FDS[i],&read_fds)){
                    string msg=recieve(FDS[i]);
                    msgs[i]=msg;
                    no_msgs_got++;
                    FD_CLR(FDS[i],&to_be_checked);
                }
            }
        }
        else
            perror("select 2");
        if(no_msgs_got == no_peers){
            break;
        }
    }

};

void send_files(int sock, vector<string> files, string directory){

    // cout << "sending these files ↓ through socket "<< sock << endl;
    // for(string filename: files){
    //     cout<< filename << ", ";
    // }
    // cout<<endl;
    for(string filename: files){
        int file_size = 0;
        FILE *my_file;

        // cout << "Calculated file size = ";
        my_file = fopen((directory + "/" + filename).c_str(), "r");
        fseek(my_file, 0, SEEK_END);
        file_size = ftell(my_file);
        // cout << file_size << endl;

        // cout << "Sending file name to the peer" << endl;
        send_to_peers(&sock, 1, filename);

        // ack here?

        // cout << "Sending file size to the peer" << endl;
        send_to_peers(&sock, 1, to_string(file_size));

        // ack here?

        char Sbuf[min(file_size, MAX_BUFFER_SIZE)];
        // cout << "Sending the file as byte array" << endl;
        fseek(my_file, 0, SEEK_END);
        file_size = ftell(my_file);
        fseek(my_file, 0, SEEK_SET); //Going to the beginning of the file

        int n = 0;
        while(!feof(my_file)){
            n = fread(Sbuf, sizeof(char), file_size, my_file);
            if (n > 0) { /* only send what has been read */
                if((n = send(sock, Sbuf, file_size, 0)) < 0) /* or (better?) send(sock, Sbuf, n, 0) */
                {
                    perror("send_data()");
                    exit(errno);
                }
            }
        }
    }
    // cout<<"exiting send"<<endl;
}

void recv_files(int sock, int no_files, string directory){
    // cout<<"entering recv for "<<no_files<<" files from"<<sock<<endl;

    for(int i=0;i<no_files;i++){
        string filename;
        recieve_from_peers(&sock, 1, &filename);
        // cout << "Received file name = "<<filename <<" from "<<sock<< endl;
        
        // ack here?

        string sfilesize;
        recieve_from_peers(&sock, 1, &sfilesize);
        // cout << "Received file size = "<< sfilesize <<" from "<<sock<< endl;
        
        // ack here?
        
        FILE *file;
        file = fopen((directory+"/Downloaded/"+filename).c_str(), "w");
        
        int n = 0;
        int file_size = stoi(sfilesize);
        char Rbuffer[min(file_size, MAX_BUFFER_SIZE)];
        // cout << "Receiving file byte array" << endl;
        while(file_size!=0){
            if ((n = recv(sock, Rbuffer, file_size, 0)) < 0){
                perror("recv_size()");
                exit(errno);
            }
            fwrite(Rbuffer, sizeof(char), n, file);
            file_size-=n;
            // cout<<"file bytes to be received "<<file_size<<" from "<<sock<<endl;
        }

        // cout << "File recieved"<<" from "<<sock<< endl;
        fclose(file);
    }
    // cout<<"exiting recv"<<" from "<<sock<<endl;

}

string eval_MD5(string fname) { 
 
  char buffer[BUFFSIZE]; 
  unsigned char digest[MD5_DIGEST_LENGTH]; 
 
  stringstream ss; 
  string md5string; 
   
  ifstream ifs(fname, ifstream::binary); 
 
  MD5_CTX md5Context; 
 
  MD5_Init(&md5Context); 
 
 
   while (ifs.good())  
    { 
 
       ifs.read(buffer, BUFFSIZE); 
 
       MD5_Update(&md5Context, buffer, ifs.gcount());  
    } 
 
   ifs.close(); 
 
   int res = MD5_Final(digest, &md5Context); 
    
    if( res == 0 ) // hash failed 
      return {};   // or raise an exception 
 
  // set up stringstream format 
  ss << hex << uppercase << setfill('0'); 
 
 
 for(unsigned char uc: digest) 
    ss << setw(2) << (int)uc; 
 
 
  md5string = ss.str(); 
  
 return md5string; 
} 



int main(int argc, char *argv[]) {
  if (argc != 3) {
        fprintf(stderr,"usage: config-file dir-name\n");
        return 1;
    }

// BEGIN workin vars
    int my_id, my_listen_port, my_unique_id, no_peers=0, no_files_to_search=0, no_files_i_have=0;
    vector<int> id_peers;
    vector<int> port_peers;
    vector<string> files_to_search, files_i_have;
// END workin vars

// reading filenames in directory
    DIR *dir; struct dirent *diread;
    if ((dir = opendir(argv[2])) != nullptr) {
        while ((diread = readdir(dir)) != nullptr) {
            if(string(diread->d_name)!="." && string(diread->d_name)!=".." && string(diread->d_name)!="Downloaded"){
                cout<<diread->d_name<<endl;
                files_i_have.push_back(diread->d_name);
                no_files_i_have++;
            }
        }
        closedir (dir);
    } else {
        perror ("opendir");
    }


// BEGIN reading workin vars
    ifstream infile(argv[1]);
    string line;
    int line_no=1;
    while (std::getline(infile, line))
    {
        if(line_no==1){
            istringstream ss(line);
            string word; // for storing each word
            int word_no=1;
            while (ss >> word) 
            {
                // print the read word
                if(word_no==1){
                    stringstream stream(word);
                    stream >> my_id;
                }
                else if(word_no==2){
                    stringstream stream(word);
                    stream >> my_listen_port;
                }
                else if(word_no==3){
                    stringstream stream(word);
                    stream >> my_unique_id;
                }
                word_no++;
            }
        }
        else if(line_no==2){
            stringstream stream(line);
            stream >> no_peers;
        }
        else if(line_no==3){
            istringstream ss(line);
            string word; // for storing each word
            int word_no=1;
            while (ss >> word) 
            {
                // print the read word
                int number;
                stringstream stream(word);
                stream >> number;

                if(word_no%2==1)
                    id_peers.push_back(number);
                else if(word_no%2==0)
                    port_peers.push_back(number);
                word_no++;
            }
        }
        else if(line_no==4){
            stringstream stream(line);
            stream >> no_files_to_search;
        }
        else{
            istringstream ss(line);
            string word;
            ss>>word;
            files_to_search.push_back(word);
        }
        line_no++;
    }

    sort(files_to_search.begin(),files_to_search.end());  
// END reading workin vars


//BEGIN setting up listener
    int listenerFD, MYPORT, BACKLOG;
    MYPORT = my_listen_port;
    BACKLOG= 100;
    

    struct sockaddr_in my_addr;
    listenerFD = socket(PF_INET, SOCK_STREAM, 0);
    // setsockopt(listenerFD, SOL_SOCKET, SO_REUSEADDR, "1", 1);

    my_addr.sin_family = AF_INET;
    my_addr.sin_port = htons(MYPORT);  
    inet_aton("127.0.0.1",&(my_addr.sin_addr));
    memset(&(my_addr.sin_zero),'\0',8);
    if(bind(listenerFD, (struct sockaddr *)&my_addr, sizeof(struct sockaddr))==-1)
        perror("bind 1");
    int fdmax = listenerFD;
    if(listen(listenerFD, BACKLOG)==-1)
        perror("listen 1");
//END setting up listener

    //BEGIN util vars
    int sin_size = sizeof(struct sockaddr_in);
    struct timeval timeout;
    timeout.tv_sec=0; 
    timeout.tv_usec=0;
    //END util vars

    int as_clientFD[no_peers]; //this gets accepted by them
    int as_serverFD[no_peers]; //these accepted by us (initially not in order)
    int as_client_maxfd=0;
    int as_server_maxfd=0;
    

    for(int i=0;i<no_peers;i++)
        as_clientFD[i]=socket(PF_INET, SOCK_STREAM, 0);

// BEGIN phase 1
    set_up_connections(listenerFD, no_peers, as_clientFD, as_serverFD, port_peers);
    for(int i=0;i<no_peers;i++){
        if(as_serverFD[i]>as_server_maxfd)
            as_server_maxfd=as_serverFD[i];
    }
    for(int i=0;i<no_peers;i++){
        if(as_clientFD[i]>as_client_maxfd)
            as_client_maxfd=as_clientFD[i];
    }
    //sending UID
    string s = to_string(my_unique_id); 
    send_to_peers(as_serverFD, no_peers, s);
   
    // recieving UIDs
    string unique_ids_servers[no_peers];
    recieve_from_peers(as_clientFD, no_peers, unique_ids_servers);
    for(int i=0;i<no_peers;i++){
        cout<<"Connected to "<<id_peers[i]<<" with unique-ID "<<unique_ids_servers[i]<<" on port "<<port_peers[i]<<endl;
    }
// END phase 1

// BEGIN phase 2
    //sending UID
    send_to_peers(as_clientFD, no_peers, s);
   
    // recieving UIDs
    string unique_ids_clientsDISORDERED[no_peers];
    recieve_from_peers(as_serverFD, no_peers, unique_ids_clientsDISORDERED);

//BEGIN rearrange the order of as_serverFD 
    for(int i=0;i<no_peers;i++){
        for(int j=i;j<no_peers;j++){
            if(unique_ids_servers[i]==unique_ids_clientsDISORDERED[j]){
                int temp;
                temp=as_serverFD[j];
                as_serverFD[j]=as_serverFD[i];
                as_serverFD[i]=temp;
                string temp2;
                temp2=unique_ids_clientsDISORDERED[j];
                unique_ids_clientsDISORDERED[j]=unique_ids_clientsDISORDERED[i];
                unique_ids_clientsDISORDERED[i]=temp;
            }
        }
    }
//END rearrange the order of as_serverFD 

    string send_to_search = "";
    for(int i=0;i<no_files_to_search;i++){
        send_to_search = send_to_search.append("$");
        send_to_search = send_to_search.append(files_to_search[i]);
    }
    send_to_search = send_to_search.append("#");
    send_to_search = send_to_search.append(to_string(my_unique_id));

    // BEGIN sharing filenames

    //sending filenames
    send_to_peers(as_serverFD,  no_peers, send_to_search);
    
    //recieving filenames and generating replies
    string requested_files[no_peers];
    string file_search_replies[no_peers];
    recieve_from_peers(as_clientFD, no_peers, requested_files);
    for(int i=0;i<no_peers;i++){
        // cout<<requested_files[i]<<endl;
        file_search_replies[i] = get_reply_for_search(requested_files[i], files_i_have);
        // cout<<file_search_replies[i]<<endl;
    }
    // END sharing filenames

    // sending replies
    send_to_peers(as_clientFD, no_peers, file_search_replies);

    //recieving replies
    string replies_got_for_file_req[no_peers];
    recieve_from_peers(as_serverFD, no_peers, replies_got_for_file_req);

    int files_found_at_uid[no_files_to_search];
    for(int j=0;j<no_files_to_search;j++){
        files_found_at_uid[j]=-1;
    }
    for(int i=0;i<no_peers;i++){
        for(int j=0;j<no_files_to_search;j++){
            if(replies_got_for_file_req[i][2*j+1]=='1'){
                if(stoi(unique_ids_servers[i])<files_found_at_uid[j]||files_found_at_uid[j]==-1){
                    files_found_at_uid[j]=stoi(unique_ids_servers[i]);
                }
            }
        }
    }
// END phase 2

// BEGIN phase 3

    string download_reqs[no_peers];
    for(int i=0;i<no_peers;i++){
        for(int j=0;j<no_files_to_search;j++){
            if(files_found_at_uid[j]==stoi(unique_ids_servers[i]))
                download_reqs[i]+=("#"+files_to_search[j]);
        }
        download_reqs[i]+="#";
    }
    send_to_peers(as_clientFD, no_peers, download_reqs);

    string uploads_to_do[no_peers];
    recieve_from_peers(as_serverFD, no_peers, uploads_to_do);


    vector<thread> file_downloads;
    vector<thread> file_uploads;

    string directory=convert_to_string(argv[2], 50);
    mkdir((directory+"/Downloaded").c_str(), 0777);
    
    for(int i=0;i<no_peers;i++){
        vector<string> files_to_upload_to_this_peer;
        while(uploads_to_do[i]!="#"){
            uploads_to_do[i] = uploads_to_do[i].substr(uploads_to_do[i].find_first_of("#")+1);
            string file_name = uploads_to_do[i].substr(0,uploads_to_do[i].find_first_of("#"));
            uploads_to_do[i] = uploads_to_do[i].substr(uploads_to_do[i].find_first_of("#"));
            files_to_upload_to_this_peer.push_back(file_name);
        }
        thread th(send_files, as_serverFD[i], files_to_upload_to_this_peer, directory);
        file_uploads.push_back(move(th));
    }
    for(int i=0;i<no_peers;i++){
        int no_files_to_download_from_this_peer=0;
        while(download_reqs[i]!="#"){
            download_reqs[i] = download_reqs[i].substr(download_reqs[i].find_first_of("#")+1);
            string file_name = download_reqs[i].substr(0,download_reqs[i].find_first_of("#"));
            download_reqs[i] = download_reqs[i].substr(download_reqs[i].find_first_of("#"));
            no_files_to_download_from_this_peer++;
        }
        thread th(recv_files, as_clientFD[i], no_files_to_download_from_this_peer, directory);
        file_downloads.push_back(move(th));
    }

    for (thread &th: file_uploads)
        th.join();
    for (thread &th: file_downloads)
        th.join();

// END phase 3
   // cout<<"end3"<<endl;
// BEGIN phase 4
    //Creating message containing what i need and what i have
    string two_hop_init_msg;
    for(int j=0;j<no_files_to_search;j++){
        if(files_found_at_uid[j]<0){
            two_hop_init_msg.append("$");
            two_hop_init_msg.append(files_to_search[j]);
        }
    }
    for(int i=0;i<no_files_i_have;i++){
        two_hop_init_msg.append("&");
        two_hop_init_msg.append(files_i_have[i]);
    }
    two_hop_init_msg.append("#");
    two_hop_init_msg.append(to_string(my_unique_id));
    
    //sending the message to peers
    send_to_peers(as_clientFD,no_peers,two_hop_init_msg);

    string two_hop_msg_rcv_reqs[no_peers];
    recieve_from_peers(as_serverFD,no_peers,two_hop_msg_rcv_reqs);

    string reply_to_2hop_msg[no_peers];
    for(int i=0;i<no_peers;i++){
        if(two_hop_msg_rcv_reqs[i][0]=='&'||two_hop_msg_rcv_reqs[i][0]=='#'){
            reply_to_2hop_msg[i].append("+");
        }
        else if(two_hop_msg_rcv_reqs[i][0]=='$'){
            string temp = two_hop_msg_rcv_reqs[i];
            while(two_hop_msg_rcv_reqs[i][0]=='$'){
                two_hop_msg_rcv_reqs[i] = two_hop_msg_rcv_reqs[i].substr((two_hop_msg_rcv_reqs[i].find_first_of("$")+1));
                int uid=-1;
                int port;
                int index;
                string file_name; 
                if(two_hop_msg_rcv_reqs[i].find_first_of("$")!=-1){
                    file_name = two_hop_msg_rcv_reqs[i].substr(0,(two_hop_msg_rcv_reqs[i].find_first_of("$")));
                    two_hop_msg_rcv_reqs[i] = two_hop_msg_rcv_reqs[i].substr(two_hop_msg_rcv_reqs[i].find_first_of("$"));
                }
                else if(two_hop_msg_rcv_reqs[i].find_first_of("&")!=-1){
                    file_name = two_hop_msg_rcv_reqs[i].substr(0,(two_hop_msg_rcv_reqs[i].find_first_of("&")));
                }
                else if(two_hop_msg_rcv_reqs[i].find_first_of("#")!=-1){
                    file_name = two_hop_msg_rcv_reqs[i].substr(0,(two_hop_msg_rcv_reqs[i].find_first_of("#")));
                }
                for(int j=0;j<no_peers;j++){
                    if(i==j){
                        continue;
                    }
                    else{
                        size_t found =  two_hop_msg_rcv_reqs[j].find("&"+file_name);
                        if (found != string::npos){
                            string temp2 = two_hop_msg_rcv_reqs[j];
                            temp2 = temp2.substr(temp2.find_first_of("#")+1);
                            if(uid>stoi(temp2)||uid==-1){
                                uid=stoi(temp2);
                            }
                        }
                    }
                }
                if(uid!=-1){
                    for(int j=0;j<no_peers;j++){
                        if(unique_ids_servers[j]==to_string(uid)){
                            port = port_peers[j];
                            index = j;
                            break;
                        }
                    }
                    reply_to_2hop_msg[i].append("#");
                    reply_to_2hop_msg[i].append(to_string(port));
                    reply_to_2hop_msg[i].append("-");
                    reply_to_2hop_msg[i].append(to_string(uid));
                    reply_to_2hop_msg[index].append("&");
                    reply_to_2hop_msg[index].append(to_string(port_peers[i]));
                }
                else{
                    reply_to_2hop_msg[i].append("+");
                }
            }
            two_hop_msg_rcv_reqs[i] = temp;
        }
    }
    send_to_peers(as_serverFD, no_peers, reply_to_2hop_msg);

    string two_hop_msg_rcv_replies[no_peers];
    recieve_from_peers(as_clientFD,no_peers,two_hop_msg_rcv_replies);

    int file_found_at_2hop_uid[no_files_to_search];
    for(int i=0;i<no_files_to_search;i++){
        file_found_at_2hop_uid[i]=-1;
    }

    for(int i=0;i<no_files_to_search;i++){
        if(files_found_at_uid[i]>0){
            continue;
        }
        else{
            for(int j=0;j<no_peers;j++){
                if((two_hop_msg_rcv_replies[j].find_first_of("+")==-1)&&(two_hop_msg_rcv_replies[j].find_first_of("#")==-1)){
                    continue;
                }
                else if((two_hop_msg_rcv_replies[j].find_first_of("+")==-1)){
                    int id=-1;
                    two_hop_msg_rcv_replies[j]=two_hop_msg_rcv_replies[j].substr((two_hop_msg_rcv_replies[j].find_first_of("-"))+1);
                    id = stoi(two_hop_msg_rcv_replies[j].substr(0,4));
                    if(id<file_found_at_2hop_uid[i]||file_found_at_2hop_uid[i]==-1){
                        file_found_at_2hop_uid[i]=id;
                    }
                }
                else if((two_hop_msg_rcv_replies[j].find_first_of("#")==-1)){
                    two_hop_msg_rcv_replies[j]=two_hop_msg_rcv_replies[j].substr(two_hop_msg_rcv_replies[j].find_first_of("+")+1);
                }
                else{
                    string temp1 = two_hop_msg_rcv_replies[j].substr(two_hop_msg_rcv_replies[j].find_first_of("#"));
                    string temp2 = two_hop_msg_rcv_replies[j].substr(two_hop_msg_rcv_replies[j].find_first_of("+"));
                    if(temp1.length()>temp2.length()){
                        int id=-1;
                        two_hop_msg_rcv_replies[j]=two_hop_msg_rcv_replies[j].substr((two_hop_msg_rcv_replies[j].find_first_of("-"))+1);
                        id = stoi(two_hop_msg_rcv_replies[j].substr(0,4));
                        if(id<file_found_at_2hop_uid[i]||file_found_at_2hop_uid[i]==-1){
                            file_found_at_2hop_uid[i]=id;
                        }
                    }
                    else{
                        two_hop_msg_rcv_replies[j]=two_hop_msg_rcv_replies[j].substr(two_hop_msg_rcv_replies[j].find_first_of("+")+1);
                    }
                }
            }
        }
    }

// END phase 4
//    cout<<"end4"<<endl;
// BEGIN phase 5
    //send list of uids i have to connect to
    string my_neighburs="#";
    for(int i=0;i<no_peers;i++)
        my_neighburs+=(unique_ids_servers[i]+"#");

    // cout<<"my_neighburs:"<<my_neighburs<<endl;
    send_to_peers(as_serverFD, no_peers, my_neighburs);

    string middle_man_neighbours[no_peers];
    recieve_from_peers(as_clientFD, no_peers, middle_man_neighbours);

    // cout<<"middle_man_neighbours↓"<<endl;
    // print_array<string>(middle_man_neighbours, no_peers);
    
    vector<int> new_uids_to_connect;
    int no_new_conns_as_downloader=0;
    for(int i=0;i<no_files_to_search;i++){
        if(file_found_at_2hop_uid[i]!=-1){
            if(find(new_uids_to_connect.begin(), new_uids_to_connect.end(), file_found_at_2hop_uid[i]) == new_uids_to_connect.end()){
                new_uids_to_connect.push_back(file_found_at_2hop_uid[i]);
                no_new_conns_as_downloader++;
            }
        }
    }
    string conn_req_to_middle_man[no_peers];
    for(int i=0;i<no_peers;i++)
        conn_req_to_middle_man[i]="#";
    for(int uid:new_uids_to_connect){
        for(int i=0;i<no_peers;i++){
            if(middle_man_neighbours[i].find(to_string(uid))!=-1){
                conn_req_to_middle_man[i]+=(to_string(uid)+"#");
                break;
            }
        }
    }

    // cout<<"conn_req_to_middle_man↓"<<endl;
    // print_array<string>(conn_req_to_middle_man, no_peers);
    send_to_peers(as_clientFD, no_peers, conn_req_to_middle_man);

    string middle_man_reqs[no_peers];
    recieve_from_peers(as_serverFD, no_peers, middle_man_reqs);
    // cout<<"middle_man_reqs↓"<<endl;
    // print_array<string>(middle_man_reqs, no_peers);

    //send no of conn they hav to accept + list of ports required for their connection
    string middle_man_replies_send[no_peers];

    for(int i=0;i<no_peers;i++){
        int x=0;
        for(int j=0;j<no_peers;j++){
            if(middle_man_reqs[j].find(unique_ids_servers[i])!=-1){
                x++;
            }
        }
        middle_man_replies_send[i]=to_string(x);
    }

    for(int i=0;i<no_peers;i++){
        string in=middle_man_reqs[i];
        string out="#";
        while(in!="#"){
            in=in.substr(in.find_first_of("#")+1);
            string uid=(in.substr(0,in.find_first_of("#")));
            in=in.substr(in.find_first_of("#"));
            int port=-1;
            for(int j=0;j<no_peers;j++){
                if(uid==unique_ids_servers[j])
                    port=port_peers[j];
            }
            if(port!=-1)
                out=out+to_string(port)+"#";
            else
                out=out+"#";
        }
        // cout<<out<<endl;
        middle_man_replies_send[i]+=out;
    }

    // cout<<"middle_man_replies_send ↓"<<endl;
    // print_array<string>(middle_man_replies_send, no_peers);

    send_to_peers(as_serverFD, no_peers, middle_man_replies_send);


    string middle_man_replies_recvd[no_peers];
    recieve_from_peers(as_clientFD, no_peers, middle_man_replies_recvd);
    // cout<<"middle_man_replies_recvd ↓"<<endl;
    // print_array<string>(middle_man_replies_recvd, no_peers);

    // cout<<"no_new_conns_as_downloader:"<<no_new_conns_as_downloader<<endl;

    int no_new_conns_as_uploader=0;
    vector<int> new_2hop_ports;
    for(int i=0;i<no_peers;i++){
        string in=middle_man_replies_recvd[i];
        no_new_conns_as_uploader += stoi(in.substr(0, in.find_first_of("#")));
        in=in.substr(in.find_first_of("#")+1);
    
        while(in!=""){
            new_2hop_ports.push_back(stoi(in.substr(0, in.find_first_of("#"))));
            in=in.substr(in.find_first_of("#")+1);
        }
    }

    // cout<<"no_new_conns_as_uploader:"<<no_new_conns_as_uploader<<endl;

    int to_connFD[no_new_conns_as_downloader];
    int from_connFD[no_new_conns_as_uploader];


    for(int i=0;i<no_new_conns_as_downloader;i++)
        to_connFD[i]=socket(PF_INET, SOCK_STREAM, 0);
    
    // cout<<"new_2hop_ports ↓"<<endl;
    // for(int port:new_2hop_ports)
    //     cout<<port<<", ";
    // cout<<endl;

    set_up_connections(listenerFD, no_new_conns_as_downloader, no_new_conns_as_uploader, to_connFD, from_connFD, new_2hop_ports);

    // cout<<"set up connection"<<endl;


    send_to_peers(from_connFD, no_new_conns_as_uploader, to_string(my_unique_id));

    string their_uid[no_new_conns_as_downloader];
    recieve_from_peers(to_connFD, no_new_conns_as_downloader, their_uid);

    string file_reqs_send_for_download[no_new_conns_as_downloader];
    for(int i=0;i<no_new_conns_as_downloader;i++){
        file_reqs_send_for_download[i]+="#";
        for(int j=0;j<no_files_to_search;j++){
            if(file_found_at_2hop_uid[j]==stoi(their_uid[i])){
                file_reqs_send_for_download[i]+=(files_to_search[j]+"#");
            }
        }
    }
    // cout<<"file_reqs_send_for_download ↓"<<endl;
    // print_array<string>(file_reqs_send_for_download, no_new_conns_as_downloader);

    send_to_peers(to_connFD, no_new_conns_as_downloader, file_reqs_send_for_download);

    string file_reqs_rcvd_for_upload[no_new_conns_as_uploader];
    recieve_from_peers(from_connFD, no_new_conns_as_uploader, file_reqs_rcvd_for_upload);
    // cout<<"file_reqs_rcvd_for_upload ↓"<<endl;
    // print_array<string>(file_reqs_rcvd_for_upload, no_new_conns_as_uploader);

    file_downloads.clear();
    file_uploads.clear();

    for(int i=0;i<no_new_conns_as_uploader;i++){
        vector<string> files_to_upload_to_this_peer;
        string x=file_reqs_rcvd_for_upload[i];
        while(x!="#"){
            x = x.substr(x.find_first_of("#")+1);
            string file_name = x.substr(0,x.find_first_of("#"));
            x = x.substr(x.find_first_of("#"));
            files_to_upload_to_this_peer.push_back(file_name);
        }
        thread th(send_files, from_connFD[i], files_to_upload_to_this_peer, directory);
        file_uploads.push_back(move(th));
    }
    for(int i=0;i<no_new_conns_as_downloader;i++){
        int no_files_to_download_from_this_peer=0;
        string x=file_reqs_send_for_download[i];
        while(x!="#"){
            x = x.substr(x.find_first_of("#")+1);
            string file_name = x.substr(0,x.find_first_of("#"));
            x = x.substr(x.find_first_of("#"));
            no_files_to_download_from_this_peer++;
        }
        thread th(recv_files, to_connFD[i], no_files_to_download_from_this_peer, directory);
        file_downloads.push_back(move(th));
    }
    // cout<<"threads called"<<endl;

    for (thread &th: file_uploads)
        th.join();
    for (thread &th: file_downloads)
        th.join();

    // cout<<"threads joined"<<endl;


    for(int j=0;j<no_files_to_search;j++){
        if(files_found_at_uid[j]>0){
            string file_name = directory+"/Downloaded/"+files_to_search[j];
            string file_hash = eval_MD5(file_name);
            transform(file_hash.begin(), file_hash.end(), file_hash.begin(), ::tolower);
            cout<<"Found "<<files_to_search[j]<<" at "<<files_found_at_uid[j]<<" with MD5 "<<file_hash<<" at depth 1"<<endl;
        }
        else if(file_found_at_2hop_uid[j]>0){
            string file_name = directory+"/Downloaded/"+files_to_search[j];
            string file_hash = eval_MD5(file_name);
            transform(file_hash.begin(), file_hash.end(), file_hash.begin(), ::tolower);
            cout<<"Found "<<files_to_search[j]<<" at "<<file_found_at_2hop_uid[j]<<" with MD5 "<<file_hash<<" at depth 2"<<endl;
        }
        else
            cout<<"Found "<<files_to_search[j]<<" at 0  with MD5 0 at depth 0"<<endl;
    }
// END phase 5

    close(listenerFD);
}