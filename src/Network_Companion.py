# import tkinter as tk
import customtkinter as tk
from tkinter import ttk
import os
from tkinter.filedialog import askopenfilename
import socket
import select
import threading as th
from tkinter import filedialog
import queue as Q
import shutil
from vidstream import AudioReceiver,AudioSender,ScreenShareClient,StreamingServer
import pyaudiowpatch
from screeninfo import get_monitors
from pynput import mouse
from pynput import keyboard
import json
from pynput.mouse import Controller as MouseController, Button as MouseButton
from pynput.keyboard import Controller as KeyboardController, Key
def donothing():
    pass
def destroy(window):
    window.destroy()
    window.update()
# Globals
base=tk.CTk()
base.resizable(False,False)
base.title("Share Files on LAN")
connection_accepted=False
serverskt=None
skt=None
start_conn=None
start_disconnect=False
reciever_thread=None
sender_thread=None
sending_queue=Q.Queue()
reciever_queue=Q.Queue()
send_size=512 << 10
recieve_size=512 << 10
pendingconfirmation_label=None
popup_file=None
file_directory=None
filename_var=None
file=None
file_send_progressbar=None
percentage_label=None
filename=None
file_send_event=th.Event()
file_send_event.set()
ack_counter_send=0
folder_sending=False
stream_popup=None
stream_label=None
stream_accept_button=None
stream_reject_button=None
default_speakers=None
streaming_var=False
start_stream_button=None
stop_stream_button=None
video_stream_check=None
mic_stream_check=None
system_audio_stream_check=None
system_mouse_stream_check=None
system_keyboard_stream_check=None
video_stream_var=tk.IntVar(value=0)
mic_stream_var=tk.IntVar(value=0)
system_audio_stream_var=tk.IntVar(value=0)
system_mouse_stream_var=tk.IntVar(value=0)
system_keyboard_stream_var=tk.IntVar(value=0)
video_stream_var_recv=False
mic_stream_var_recv=False
system_audio_stream_var_recv=False
system_mouse_stream_var_recv=False
system_keyboard_stream_var_recv=False
video_stream_sender=None
mic_audio_stream_sender=None
mouse_stream_controller = MouseController()
mouse_stream_listener=None
mouse_stream_sender_socket=None
mouse_stream_receive_server=None
mouse_stream_receive_socket=None
mouse_thread=None
keyboard_stream_controller = KeyboardController()
keyboard_stream=None
keyboard_stream_listener=None
keyboard_stream_sender_socket=None
keyboard_stream_receive_server=None
keyboard_stream_receive_socket=None
keyboard_thread=None
system_audio_stream_sender_thread=None
video_stream_reciever=None
mic_audio_stream_reciever=None
system_audio_reciever=None
system_audio_socket=None
stop_system_audio_event=th.Event()
stop_system_audio_event.set()
received_rate=None
pause_transfer_button=None
stop_transfer_button=None
pause_file_send=th.Event()
pause_file_send.set()
stop_file_send=th.Event()
stop_file_send.set()
stop_file_recv=th.Event()
stop_file_recv.set()
max_x_res=0
max_y_res=0
for m in get_monitors():
    if m.is_primary:
        max_x_res=m.width
        max_y_res=m.height
        break
video_stream_res_x_var=tk.StringVar()
video_stream_res_y_var=tk.StringVar()
video_stream_res_x_entry=None
video_stream_res_y_entry=None
video_stream_res_x_label=None
video_stream_res_y_label=None
video_stream_label=None
incoming_address=None
# Streaming Functions
def send_system_audio(in_data, frame_count, time_info, status):
    global system_audio_socket
    try:
        system_audio_socket.sendall(in_data)
    except ConnectionAbortedError:
        stop_system_audio_event.set()
    except ConnectionResetError:
        stop_system_audio_event.set()
    return (in_data,pyaudiowpatch.paContinue)

def send_mouse_keyboard_data(event_type, data):
    global mouse_stream_sender_socket, keyboard_stream_sender_socket
    event = {"type": event_type, **data}
    if(event_type == "key_press" or event_type=="key_release"):
        print("sending keyboard",event)
        select.select([],[keyboard_stream_sender_socket.fileno()],[])
        keyboard_stream_sender_socket.sendall((json.dumps(event) + "\n").encode())
    else:
        select.select([],[mouse_stream_sender_socket.fileno()],[])
        mouse_stream_sender_socket.sendall((json.dumps(event) + "\n").encode())

def mouse_on_move(x, y):
    th.Thread(target=send_mouse_keyboard_data, args=("mouse_move", {"x": x, "y": y})).start()

def mouse_on_click(x, y, button, pressed):
    th.Thread(target=send_mouse_keyboard_data, args=("mouse_click", {"x": x, "y": y, "button": str(button), "pressed": pressed})).start()
    
def keyboard_on_press(key):
    try:
        # if key.char:
        th.Thread(target=send_mouse_keyboard_data, args=("key_press", {"key": str(key)})).start()
    except AttributeError:
        th.Thread(target=send_mouse_keyboard_data, args=("key_press", {"key": str(key)})).start()

def keyboard_on_release(key):
    try:
        # if key.char:
        th.Thread(target=send_mouse_keyboard_data, args=("key_release", {"key": str(key)})).start()
    except AttributeError:
        th.Thread(target=send_mouse_keyboard_data, args=("key_release", {"key": str(key)})).start()
        
def start_sender_mouse_stream():
    global mouse_stream_sender_socket, mouse_stream_listener
    mouse_stream_sender_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    mouse_stream_sender_socket.connect((incoming_address,int(1900)))
    mouse_stream_listener = mouse.Listener(on_move=mouse_on_move, on_click=mouse_on_click)
    mouse_stream_listener.start()
    
def start_receive_mouse_stream():
    global mouse_stream_receive_server,mouse_stream_receive_socket,mouse_thread
    mouse_stream_receive_server = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    mouse_stream_receive_server.bind(('',1900))
    mouse_stream_receive_server.listen(1)
    try:
        mouse_stream_receive_socket, incoming_addr=mouse_stream_receive_server.accept()
        mouse_thread=th.Thread(target=receive_mouse, args=())
        mouse_thread.start()
    except OSError:
        return
    
def receive_mouse():
    global mouse_stream_receive_socket,mouse_stream_controller, system_mouse_stream_var, mouse_stream_receive_server
    while system_mouse_stream_var.get():
        try:
            select.select([mouse_stream_receive_socket.fileno()],[],[])
            recieve_data=mouse_stream_receive_socket.recv(1024)
            if not recieve_data:
                return
            events = recieve_data.decode().strip().split("\n")
            for event in events:
                event_data = json.loads(event)
                # print(event_data)
                # continue
                if event_data["type"] == "mouse_move":
                    mouse_stream_controller.position = (event_data["x"], event_data["y"])

                elif event_data["type"] == "mouse_click":
                    btn = MouseButton.left if event_data["button"] == "Button.left" else MouseButton.right
                    if event_data["pressed"]:
                        mouse_stream_controller.press(btn)
                    else:
                        mouse_stream_controller.release(btn)
        except ValueError:
            # print("ValueError")
            continue
        except OSError:
            # print("OSError")
            continue
    mouse_stream_receive_socket.close()
    mouse_stream_receive_server.close()

def start_sender_keyboard_stream():
    global keyboard_stream_sender_socket, keyboard_stream_listener
    keyboard_stream_sender_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    keyboard_stream_sender_socket.connect((incoming_address,int(2000)))
    keyboard_stream_listener = keyboard.Listener(on_press=keyboard_on_press, on_release=keyboard_on_release)
    keyboard_stream_listener.start()
    
def start_receive_keyboard_stream():
    global keyboard_stream_receive_server,keyboard_stream_receive_socket,keyboard_thread
    keyboard_stream_receive_server = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    keyboard_stream_receive_server.bind(('',2000))
    keyboard_stream_receive_server.listen(1)
    try:
        keyboard_stream_receive_socket, incoming_addr=keyboard_stream_receive_server.accept()
        keyboard_thread=th.Thread(target=receive_keyboard, args=())
        keyboard_thread.start()
    except OSError:
        return
        
def receive_keyboard():
    global keyboard_stream_receive_socket,keyboard_stream_controller, system_keyboard_stream_var, keyboard_stream_receive_server
    while system_keyboard_stream_var.get():
        try:
            select.select([keyboard_stream_receive_socket.fileno()],[],[])
            recieve_data=keyboard_stream_receive_socket.recv(1024)
            if not recieve_data:
                return
            events = recieve_data.decode().strip().split("\n")
            for event in events:
                event_data = json.loads(event)
                key_str = event_data["key"]
                try:
                    if key_str.startswith("Key."):
                        key = getattr(Key, key_str[4:])
                    else:
                        key = key_str
                except AttributeError:
                    key = key_str  # fallback if it's an unknown special key

                if event_data["type"] == "key_press":
                    keyboard_stream_controller.press(key)
                elif event_data["type"] == "key_release":
                    keyboard_stream_controller.release(key)
        except ValueError:
            # print("ValueError")
            continue
        except OSError:
            # print("OSError")
            continue
    keyboard_stream_receive_socket.close()
    keyboard_stream_receive_server.close()
        
def system_audio_sender():
    global default_speakers,streaming_var,system_audio_stream,system_audio_socket
    connectionestablished=False
    system_audio_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    while not connectionestablished:
        try:
            system_audio_socket.connect((ip1_var.get()+'.'+ip2_var.get()+'.'+ip3_var.get()+'.'+ip4_var.get(),1800))
            connectionestablished=True
        except ConnectionRefusedError:
            return
        except TimeoutError:
            return
    with pyaudiowpatch.PyAudio().open(format=pyaudiowpatch.paInt16,channels=1,rate=int(default_speakers["defaultSampleRate"]),
                                      input=True,frames_per_buffer=4096,input_device_index=default_speakers["index"],
                                        stream_callback=send_system_audio) as system_audio_stream:
        stop_system_audio_event.wait()
    system_audio_socket.close()
def client_streaming():
    global video_stream_sender,mic_audio_stream_sender,system_audio_stream_sender_thread
    if video_stream_var.get():
        video_stream_sender=ScreenShareClient(host=ip1_var.get()+'.'+ip2_var.get()+'.'+ip3_var.get()+'.'+ip4_var.get(),port=1600,x_res=int(video_stream_res_x_var.get()),y_res=int(video_stream_res_y_var.get()))
        video_stream_sender.start_stream()
    if mic_stream_var.get():
        mic_audio_stream_sender=AudioSender(host=ip1_var.get()+'.'+ip2_var.get()+'.'+ip3_var.get()+'.'+ip4_var.get(),port=1700)
        mic_audio_stream_sender.start_stream()
    if system_audio_stream_var.get():
        stop_system_audio_event.clear()
        system_audio_stream_sender_thread=th.Thread(target=system_audio_sender,args=())
        system_audio_stream_sender_thread.start()
    if system_mouse_stream_var.get():
        temp_thread_mouse = th.Thread(target=start_receive_mouse_stream,args=())
        temp_thread_mouse.start()
    if system_keyboard_stream_var.get():
        temp_thread_keyboard = th.Thread(target=start_receive_keyboard_stream,args=())
        temp_thread_keyboard.start()
def host_streaming():
    global video_stream_reciever,mic_audio_stream_reciever,system_audio_reciever,received_rate
    if video_stream_var_recv:
        video_stream_reciever=StreamingServer(host=ip1_var.get()+'.'+ip2_var.get()+'.'+ip3_var.get()+'.'+ip4_var.get(),port=1600)
        video_stream_reciever.start_server()
    if mic_stream_var_recv:
        mic_audio_stream_reciever=AudioReceiver(host=ip1_var.get()+'.'+ip2_var.get()+'.'+ip3_var.get()+'.'+ip4_var.get(),port=1700)
        mic_audio_stream_reciever.start_server()
    if system_audio_stream_var_recv:
        system_audio_reciever=AudioReceiver(host=ip1_var.get()+'.'+ip2_var.get()+'.'+ip3_var.get()+'.'+ip4_var.get(),port=1800,rate=int(received_rate))
        system_audio_reciever.start_server()
    if system_mouse_stream_var_recv:
        start_sender_mouse_stream()
    if system_keyboard_stream_var_recv:
        start_sender_keyboard_stream()      
# File Send Function
def file_send():    
    global file_send_progressbar,file_send_event,ack_counter_send,file_directory,folder_sending,file
    global pause_file_send,stop_file_send
    first_time=True
    stop_file_send.set()
    pause_file_send.set()
    if folder_sending:
        size=filename_var.get().split("\n")[1]
        size=int(size[1:-7])
        totalsize=size
        sent=0
        pause_transfer_button.configure(state="normal")
        stop_transfer_button.configure(state="normal")
        for path, dirs, files in os.walk(file_directory):
            for f in files:
                ack_counter_send=0
                fp = os.path.join(path, f)
                size=os.path.getsize(fp)
                file=open(fp,"rb")
                if first_time:
                    file_send_event.clear()
                    if len(path.split('\\',1))==1:
                        sending_queue.put("<<NODIR>>".encode())
                    else:
                        sending_queue.put(path.split('\\',1)[1].encode())
                    first_time=False
                else:
                    select.select([],[skt.fileno()],[])
                    if len(path.split('\\',1))==1:
                        skt.sendall("<<NODIR>>".encode())
                    else:
                        skt.sendall(path.split('\\',1)[1].encode())
                # print("Sent Directory")
                select.select([skt.fileno()],[],[])
                check_ack_recv=skt.recv(7)
                if check_ack_recv.decode()!="<<ACK>>":
                    print("error-"+check_ack_recv.decode())
                # print("Recieved ACK")
                select.select([],[skt.fileno()],[])
                skt.sendall((f+'\n('+str(size)+' Bytes)').encode())
                # print("Sent Filename")
                select.select([skt.fileno()],[],[])
                check_ack_recv=skt.recv(7)
                if check_ack_recv.decode()!="<<ACK>>":
                    print("error-"+check_ack_recv.decode())
                # print("Recieved ACK")
                while data:=file.read(send_size):
                    if not pause_file_send.is_set():
                        if len(data)<len("<<PAUSETRANSFER>>".encode()) or len(data)<len("<<RESUMETRANSFER>>".encode()):
                            pause_file_send.set()
                        else:
                            pendingconfirmation_label.configure(text="Folder transfer Paused")
                            skt.sendall("<<PAUSETRANSFER>>".encode())
                            pause_transfer_button.configure(text="Resume")
                            pause_file_send.wait()
                            if stop_file_send.is_set():
                                skt.sendall("<<RESUMETRANSFER>>".encode())
                                pause_transfer_button.configure(text="Pause")
                                pendingconfirmation_label.configure(text="Sending...")
                    if not stop_file_send.is_set():
                        if len(data)<len("<<STOPTRANSFER>>".encode()):
                            stop_file_send.set()
                        else:
                            skt.sendall("<<STOPTRANSFER>>".encode())
                            file.close()
                            pendingconfirmation_label.configure(text="Folder transfer Cancelled\n(You can close this window)")
                            popup_file.protocol("WM_DELETE_WINDOW",lambda: destroy(popup_file))
                            file_send_event.set()
                            stop_file_send.set()
                            pause_transfer_button.configure(state="disabled")
                            stop_transfer_button.configure(state="disabled")
                            return
                    select.select([],[skt.fileno()],[])
                    skt.sendall(data)
                    ack_counter_send+=1
                    sent+=len(data)
                    file_send_progressbar.set((sent/totalsize))
                    percentage_label.configure(text=str(int((sent/totalsize)*100))+"%")
                    if ack_counter_send==100:
                        select.select([skt.fileno()],[],[])
                        check_ack_recv=skt.recv(7)
                        if check_ack_recv.decode()!="<<ACK>>":
                            print("error-"+check_ack_recv.decode())
                        ack_counter_send=0
                select.select([],[skt.fileno()],[])
                skt.sendall("<<EOF>>".encode())
                # print("Sent EOF")
                select.select([skt.fileno()],[],[])
                check_ack_recv=skt.recv(7)
                if check_ack_recv.decode()!="<<ACK>>":
                    print("error-"+check_ack_recv.decode())
                # print("Recieved ACK")
                file.close()
        pendingconfirmation_label.configure(text="Folder Sent(You can close this window now)")
        popup_file.protocol("WM_DELETE_WINDOW",lambda: destroy(popup_file))
        file_send_event.set()
    else:
        size=filename_var.get().split("\n")[1]
        size=int(size[1:-7])
        totalsize=size
        sent=0
        first_time=True
        ack_counter_send=0
        pause_transfer_button.configure(state="normal")
        stop_transfer_button.configure(state="normal")
        while data:=file.read(send_size):
            if not pause_file_send.is_set():
                    if len(data)<len("<<PAUSETRANSFER>>".encode()) or len(data)<len("<<RESUMETRANSFER>>".encode()):
                        pause_file_send.set()
                    else:
                        pendingconfirmation_label.configure(text="File transfer Paused")
                        skt.sendall("<<PAUSETRANSFER>>".encode())
                        pause_transfer_button.configure(text="Resume")
                        pause_file_send.wait()
                        if stop_file_send.is_set():
                            skt.sendall("<<RESUMETRANSFER>>".encode())
                            pause_transfer_button.configure(text="Pause")
                            pendingconfirmation_label.configure(text="Sending...")
            if not stop_file_send.is_set():
                if len(data)<len("<<STOPTRANSFER>>".encode()):
                    stop_file_send.set()
                else:
                    pause_file_send.set()
                    skt.sendall("<<STOPTRANSFER>>".encode())
                    file.close()
                    pendingconfirmation_label.configure(text="File transfer Cancelled\n(You can close this window)")
                    popup_file.protocol("WM_DELETE_WINDOW",lambda: destroy(popup_file))
                    file_send_event.set()
                    stop_file_send.set()
                    pause_transfer_button.configure(state="disabled")
                    stop_transfer_button.configure(state="disabled")
                    return
            if first_time:
                file_send_event.clear()
                sending_queue.put(data)
                first_time=False
            else:
                select.select([],[skt.fileno()],[])
                skt.sendall(data)
            if ack_counter_send==100:
                select.select([skt.fileno()],[],[])
                check_ack_recv=skt.recv(7)
                if check_ack_recv.decode()!="<<ACK>>":
                    print("error-"+check_ack_recv.decode())
                ack_counter_send=0
            ack_counter_send+=1
            sent+=len(data)
            file_send_progressbar.set((sent/totalsize))
            percentage_label.configure(text=str(int((sent/totalsize)*100))+"%")
        select.select([],[skt.fileno()],[])
        skt.sendall("<<EOF>>".encode())
        select.select([skt.fileno()],[],[])
        check_ack_recv=skt.recv(7)
        if check_ack_recv.decode()!="<<ACK>>":
            print("error-"+check_ack_recv.decode())
        file.close()
        pendingconfirmation_label.configure(text="File Sent(You can close this window now)")
        popup_file.protocol("WM_DELETE_WINDOW",lambda: destroy(popup_file))
        file_send_event.set()
    stop_transfer_button.configure(state="disabled")
    pause_transfer_button.configure(state="disabled")
    return
# File Recieve Function
def file_recieve(initialdata):
    global filename,sending_queue,ack_counter_send,file_directory,file,folder_sending,stop_transfer_button
    first_time=True
    stop_file_send.set()
    pause_file_send.set()
    if folder_sending:
        ack_counter_send=0
        size=filename.split("\n")[1]
        size=int(size[1:-7])
        totalsize=size
        while size:
            ack_counter_send=0
            ack_counter_send_checker = 512 << 10
            if initialdata:
                if initialdata.decode()!="<<NODIR>>":
                    try:
                        os.mkdir(os.path.join(file_directory,initialdata.decode()))
                    except FileExistsError:
                        pass
                    temp_file_directory=os.path.join(file_directory,initialdata.decode())
                else:
                    temp_file_directory=file_directory
                initialdata=None
            else:
                select.select([skt.fileno()],[],[])
                data=skt.recv(recieve_size)
                if data.decode()!="<<NODIR>>":
                    try:
                        os.mkdir(os.path.join(file_directory,data.decode()))
                    except FileExistsError:
                        pass
                    temp_file_directory=os.path.join(file_directory,data.decode())
                else:
                    temp_file_directory=file_directory
            if first_time:
                file_send_event.clear()
                sending_queue.put("<<ACK>>".encode())
                first_time=False
            else:
                select.select([],[skt.fileno()],[])
                skt.sendall("<<ACK>>".encode())
            # print("Sent ACK of directory")
            select.select([skt.fileno()],[],[])
            data=skt.recv(recieve_size).decode()
            file=open(temp_file_directory+"/"+data.split('\n')[0],"wb")
            temp_sizeoffile=int(data.split('\n')[1][1:-7])
            select.select([],[skt.fileno()],[])
            skt.sendall("<<ACK>>".encode())
            # print("Sent ACK of filename")
            while temp_sizeoffile:
                select.select([skt.fileno()],[],[])
                data=skt.recv(min(recieve_size,temp_sizeoffile))
                try:
                    if data.decode()=="<<PAUSETRANSFER>>":
                        pause_file_send.clear()
                    elif data.decode()=="<<STOPTRANSFER>>":
                        stop_file_send.clear()
                except UnicodeDecodeError:
                    pass
                if not pause_file_send.is_set():
                    pendingconfirmation_label.configure(text="Folder transfer Paused by Client")
                    select.select([skt.fileno()],[],[])
                    data=skt.recv(min(recieve_size,temp_sizeoffile))
                    try:
                        if data.decode()=="<<RESUMETRANSFER>>":
                            pause_file_send.set()
                            pendingconfirmation_label.configure(text="Receiving...")
                            select.select([skt.fileno()],[],[])
                            data=skt.recv(min(recieve_size,temp_sizeoffile))
                        elif data.decode()=="<<STOPTRANSFER>>":
                            pause_file_send.set()
                            stop_file_send.clear()
                    except UnicodeDecodeError:
                        pass
                if not stop_file_send.is_set():
                    file.close()
                    pendingconfirmation_label.configure(text="Folder transfer Cancelled\n(You can close this window)")
                    file_status.configure(text="Folder Tranfer Unsuccessful")
                    popup_file.protocol("WM_DELETE_WINDOW",lambda: destroy(popup_file))
                    file_send_event.set()
                    stop_file_send.set()
                    folder_sending=False
                    return
                if data:
                    file.write(data)
                    ack_counter_send_checker-=len(data)
                    if ack_counter_send_checker<=0:
                        ack_counter_send+=1
                        ack_counter_send_checker += 512 << 10
                    size-=len(data)
                    temp_sizeoffile-=len(data)
                    file_send_progressbar.set(((totalsize-size)/(totalsize)))
                    percentage_label.configure(text=str(int(((totalsize-size)/(totalsize))*100))+"%")
                    if ack_counter_send==100:
                        select.select([],[skt.fileno()],[])
                        skt.sendall("<<ACK>>".encode())
                        ack_counter_send=0
            select.select([skt.fileno()],[],[])
            data=skt.recv(recieve_size)
            if data.decode()=="<<EOF>>":
                select.select([],[skt.fileno()],[])
                skt.sendall("<<ACK>>".encode())
                # print("Sent ACK of EOF")
            file.close()
        folder_sending=False
        pendingconfirmation_label.configure(text="Folder Recieved(You can close this window now)")
        popup_file.protocol("WM_DELETE_WINDOW",lambda: destroy(popup_file))
        file_status.configure(text="Status: Folder Recieved Succesfully")
        file_send_event.set()
    else:
        ack_counter_send=0
        size=filename.split("\n")[1]
        size=int(size[1:-7])
        totalsize=size
        file.write(initialdata)
        ack_counter_send+=1
        size-=len(initialdata)
        file_send_progressbar.set(((totalsize-size)/(totalsize)))
        percentage_label.configure(text=str(int(((totalsize-size)/(totalsize))*100))+"%")
        ack_counter_send_checker = 512 << 10
        # sending_queue.put("<<ACK>>".encode())
        while size:
            select.select([skt.fileno()],[],[])
            data=skt.recv(min(recieve_size,size))
            try:
                if data.decode()=="<<PAUSETRANSFER>>":
                    pause_file_send.clear()
                elif data.decode()=="<<STOPTRANSFER>>":
                    stop_file_send.clear()
            except UnicodeDecodeError:
                pass
            if not pause_file_send.is_set():
                pendingconfirmation_label.configure(text="File transfer Paused by Client")
                select.select([skt.fileno()],[],[])
                data=skt.recv(min(recieve_size,size))
                if data.decode()=="<<RESUMETRANSFER>>":
                    pause_file_send.set()
                    pendingconfirmation_label.configure(text="Receiving...")
                    select.select([skt.fileno()],[],[])
                    data=skt.recv(min(recieve_size,size))
                elif data.decode()=="<<STOPTRANSFER>>":
                    pause_file_send.set()
                    stop_file_send.clear()
            if not stop_file_send.is_set():
                pause_file_send.set()
                file.close()
                pendingconfirmation_label.configure(text="File transfer Cancelled\n(You can close this window)")
                file_status.configure(text="File Tranfer Unsuccessful")
                popup_file.protocol("WM_DELETE_WINDOW",lambda: destroy(popup_file))
                file_send_event.set()
                stop_file_send.set()
                return
            if data:
                file.write(data)
                size-=len(data)
                ack_counter_send_checker-=len(data)
                if ack_counter_send_checker<=0:
                    ack_counter_send+=1
                    ack_counter_send_checker += 512 << 10
                file_send_progressbar.set(((totalsize-size)/(totalsize)))
                percentage_label.configure(text=str(int(((totalsize-size)/(totalsize))*100))+"%")
                if ack_counter_send==100:
                    if first_time:
                        file_send_event.clear()
                        sending_queue.put("<<ACK>>".encode())
                        first_time=False
                    else:
                        select.select([],[skt.fileno()],[])
                        skt.sendall("<<ACK>>".encode())
                    ack_counter_send=0
        select.select([skt.fileno()],[],[])
        data=skt.recv(recieve_size)
        if data.decode()=="<<EOF>>":
            select.select([],[skt.fileno()],[])
            skt.sendall("<<ACK>>".encode())
        file.close()
        pendingconfirmation_label.configure(text="File Recieved(You can close this window now)")
        popup_file.protocol("WM_DELETE_WINDOW",lambda: destroy(popup_file))
        file_status.configure(text="Status: File Recieved Succesfully")
        file_send_event.set()
    return
def rejectfile(dropdown_var):
    global pendingconfirmation_label,popup_file,folder_sending
    if dropdown_var and dropdown_var.get()=="Host(Recieve File)":
        sending_queue.put("<<REJECTFILE>>".encode())
        folder_sending=False
        popup_file.destroy()
        popup_file.update()
    else:
        pendingconfirmation_label.configure(text="Host Rejected(You can close this Window)")
        popup_file.protocol("WM_DELETE_WINDOW",lambda : destroy(popup_file))
def acceptfile(acceptfile_butt,rejectfile_butt):
    global file,file_directory,pendingconfirmation_label,filename,popup_file,stop_transfer_button
    if dropdown_var.get()=="Client(Send File)":
        if folder_sending:
            pendingconfirmation_label.configure(text="Sending...")
            file_send()
        else:
            file=open(file_directory,"rb")
            pendingconfirmation_label.configure(text="Sending...")
            file_send()
    else:
        pendingconfirmation_label.configure(text="Receiving...")
        file_directory=filename_var.get()[10:]
        if folder_sending:
            try:
                os.mkdir(os.path.join(file_directory,filename.split("\n")[0]))
            except FileExistsError:
                pass
            file_directory=os.path.join(file_directory,filename.split("\n")[0])
        else:
            file=open(file_directory+"/"+filename.split("\n")[0],"wb")
        sending_queue.put("<<ACCEPTFILE>>".encode())
        acceptfile_butt.configure(state="disabled")
        rejectfile_butt.configure(state="disabled")
# Sender/Reciever Working
def reciever():
    global skt,start_disconnect,reciever_queue,recieve_size,filename,ack_counter_send,folder_sending,streaming_var
    global pause_file_send,stop_file_send
    while True:
        try:
            select.select([skt.fileno()],[],[])
            recieve_data=skt.recv(recieve_size)
            if not recieve_data:
                start_disconnect=True
                disconnect()
                return
            try:
                backend_data=recieve_data.decode()
                if backend_data=="<<DISCONNECT>>":
                    disconnect()
                    return
                elif backend_data.split("//")[0]=="<<FILEREQUEST>>":
                    filename=recieve_data.decode().split("//")[1]
                    sendfile()
                elif backend_data.split("//")[0]=="<<FOLDERREQUEST>>":
                    folder_sending=True
                    filename=recieve_data.decode().split("//")[1]
                    sendfile()
                elif backend_data=="<<ACCEPTFILE>>":
                    acceptfile(None,None)
                elif backend_data=="<<REJECTFILE>>":
                    rejectfile(None)
                elif backend_data[:17]=="<<REQUESTSTREAM>>":
                    stream(data=backend_data)
                elif backend_data=="<<REJECTSTREAM>>":
                    rejectstream()
                elif backend_data=="<<ACCEPTSTREAM>>":
                    acceptstream()
                elif backend_data=="<<ENDSTREAM>>":
                    streaming_var=False
                    stop_stream()
                else:
                    file_recieve(recieve_data)
            except UnicodeDecodeError:
                file_recieve(recieve_data)
        except ValueError:
            # print("ValueError")
            return
        except OSError:
            # print("OSError")
            return
def sender():
    global skt,sending_queue,file_send_event
    while True:
        try:
            select.select([],[skt.fileno()],[])
            if start_disconnect:
                skt.sendall("<<DISCONNECT>>".encode())
                return
            if sending_queue.qsize()!=0:
                skt.sendall(sending_queue.get())
                file_send_event.wait()
        except ValueError:
            # print("ValueError")
            return
        except OSError:
            # print("OSError")
            return
# Working
def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ans=s.getsockname()
    s.close()
    return ans
def dropdown_check(value):
    global skt,serverskt
    if value=="Client(Send File)":
        connect_start_var.set("Connect")
        ip1.configure(state="normal")
        ip2.configure(state="normal")
        ip3.configure(state="normal")
        ip4.configure(state="normal")
        ip1_var.set('')
        ip2_var.set('')
        ip3_var.set('')
        ip4_var.set('')
        port_var.set('')
        port.configure(state="normal")
        filename_var.set("Disconnected")
        ipaddr_text.configure(text='Enter IP Address of Host')
        port_text.configure(text='Enter Port of Host')
        file_button.configure(text="Select File")
        file_button.configure(width=75)
        file_button.grid_forget()
        file_button.grid(row=4,column=0,columnspan=9,pady=2,sticky='w',padx=0)
        selectfolder_button.grid(row=4,column=0,columnspan=9,pady=2,sticky='e',padx=0)
        stream_button.grid(row=4,column=4,columnspan=6,pady=2,sticky='w',padx=10)
        file_status.grid_forget()
        sendfile_button.grid(row=6,column=0,columnspan=9,pady=2)
        skt=socket.socket()
    else:
        connect_start_var.set("Start")
        ip=get_ip_address()[0].split('.')
        ip1.configure(state="disabled")
        ip2.configure(state="disabled")
        ip3.configure(state="disabled")
        ip4.configure(state="disabled")
        ip1_var.set(ip[0])
        ip2_var.set(ip[1])
        ip3_var.set(ip[2])
        ip4_var.set(ip[3])
        port_var.set('')
        port.configure(state="normal")
        filename_var.set("Not Started")
        ipaddr_text.configure(text='IP Address')
        file_button.configure(text="Select Download Location")
        file_button.grid_forget()
        file_button.grid(row=4,column=0,columnspan=9,pady=2)
        file_button.configure(width=200)
        port_text.configure(text='Port\nRange :- 2000 to 49151')
        sendfile_button.grid_forget()
        stream_button.grid_forget()
        selectfolder_button.grid_forget()
        file_status.grid(row=6,column=0,columnspan=9,pady=2)
        serverskt=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
# Validation
def validateip(value):
    if not value:
            return True
    try:
        if int(value)>=0 and int(value)<=255 and len(value)<=3:
            return True
        else:
            return False
    except ValueError:
        return False
def validateport(value):
    if not value:
            return True
    try:
        if int(value)>=0 and int(value)<=65535 and len(value)<=5:
            return True
        else:
            return False
    except ValueError:
        return False
validateipreg=base.register(validateip)
validateportreg=base.register(validateport)
# Elements
base.rowconfigure(index=1,minsize=50)
dropdown_var = tk.StringVar(value="Select User")
dropdown_label = tk.CTkLabel(base, text="Host")
dropdown=tk.CTkOptionMenu(base,variable=dropdown_var,values=["Host(Recieve File)","Client(Send File)"],command=dropdown_check)
ip1_var=tk.StringVar()
ip2_var=tk.StringVar()
ip3_var=tk.StringVar()
ip4_var=tk.StringVar()
port_var=tk.StringVar(value="")
ip1=tk.CTkEntry(base,textvariable=ip1_var,width=36,justify="center",validate="all",validatecommand=(validateipreg,'%P'),state="disabled")
ip2=tk.CTkEntry(base,textvariable=ip2_var,width=36,justify="center",validate="all",validatecommand=(validateipreg,'%P'),state="disabled")
ip3=tk.CTkEntry(base,textvariable=ip3_var,width=36,justify="center",validate="all",validatecommand=(validateipreg,'%P'),state="disabled")
ip4=tk.CTkEntry(base,textvariable=ip4_var,width=36,justify="center",validate="all",validatecommand=(validateipreg,'%P'),state="disabled")
port=tk.CTkEntry(base,textvariable=port_var,width=60,justify="center",validate="all",validatecommand=(validateportreg,'%P'),state="disabled")
fullstop=[tk.CTkLabel(base,text='.'),tk.CTkLabel(base,text='.'),tk.CTkLabel(base,text='.')]
colon=tk.CTkLabel(base,text=':')
connect_start_var=tk.StringVar(value="Connect")
connect_start=tk.CTkButton(base,textvariable=connect_start_var,state="disabled")
ipaddr_text=tk.CTkLabel(base,text='')
port_text=tk.CTkLabel(base,text='')
filename_var=tk.StringVar(value='Select User Type')
filename_text=tk.CTkLabel(base,textvariable=filename_var,wraplength=400)
file_button=tk.CTkButton(base,text='',state='disabled',width=75)
file_status=tk.CTkLabel(base,text='Status: ')
sendfile_button=tk.CTkButton(base,text='Send',state='disabled')
selectfolder_button=tk.CTkButton(base,text='Select Folder',state='disabled',width=75)
disconnect_button=tk.CTkButton(base,text='Disconnect',state='disabled')
stream_button=tk.CTkButton(base,text='Stream',state='disabled',width=75)
# Placing Elements
dropdown.grid(row=0,column=0,columnspan=10)
ipaddr_text.grid(row=1,column=0,columnspan=8)
port_text.grid(row=1,column=8)
ip1.grid(row=2,column=0,padx=5)
ip2.grid(row=2,column=2,padx=5)
ip3.grid(row=2,column=4,padx=5)
ip4.grid(row=2,column=6,padx=5)
port.grid(row=2,column=8,padx=5)
fullstop[0].grid(row=2,column=1)
fullstop[1].grid(row=2,column=3)
fullstop[2].grid(row=2,column=5)
colon.grid(row=2,column=7)
connect_start.grid(row=3,column=0,columnspan=9,pady=2)
file_button.grid(row=4,column=0,columnspan=9,pady=2)
filename_text.grid(row=5,column=0,columnspan=9,pady=2)
disconnect_button.grid(row=7,column=0,columnspan=9,pady=2)
# Trace
def check_connectstart(*args):
    ip_check = [ip1_var.get(),ip2_var.get(),ip3_var.get(),ip4_var.get()]
    check=0
    for x in ip_check:
        if x and int(x)>=0 and int(x)<=255:
            check+=1
    if port_var.get() and int(port_var.get())<=65535 and int(port_var.get())>=2000 and check==4:
        connect_start.configure(state="normal")
    else:
        connect_start.configure(state="disabled")
ip1_var.trace_add("write",callback=check_connectstart)
ip2_var.trace_add("write",callback=check_connectstart)
ip3_var.trace_add("write",callback=check_connectstart)
ip4_var.trace_add("write",callback=check_connectstart)
port_var.trace_add("write",callback=check_connectstart)
# Working
def client_connection(connecting_label,popup):
    global skt,connection_accepted,reciever_thread,sender_thread
    try:
        connecting_label.configure(text='Connecting...'+"\n(Your Address : "+get_ip_address()[0]+")")
        connecting_label.update()
        skt.connect((ip1_var.get()+'.'+ip2_var.get()+'.'+ip3_var.get()+'.'+ip4_var.get(),int(port_var.get())))
    except ConnectionRefusedError:
        connecting_label.configure(text="Connection Refused")
        skt.close()
        skt=socket.socket()
        popup.protocol("WM_DELETE_WINDOW",lambda: destroy(popup))
        return
    except TimeoutError:
        connecting_label.configure(text="Connection Timed Out")
        skt.close()
        skt=socket.socket()   
        popup.protocol("WM_DELETE_WINDOW",lambda: destroy(popup))
        return
    except OSError:
        skt.close()
        skt=socket.socket()
        connecting_label.configure(text="Incorrect IP/Port")
        popup.protocol("WM_DELETE_WINDOW",lambda: destroy(popup))
        return
    try:
        select.select([skt.fileno()],[],[])
        firstrecv=skt.recv(1024).decode()
        if(firstrecv=='accepted'):
            connection_accepted=True
            connecting_label.configure(text="Connection Accepted\n(Close this window)")
            connect_start.configure(state="disabled")
            file_button.configure(state="normal")
            selectfolder_button.configure(state="normal")
            stream_button.configure(state="normal")
            filename_var.set("No File Selected")
            disconnect_button.configure(state="normal")
            dropdown.configure(state="disabled")
            ip1.configure(state="disabled")
            ip2.configure(state="disabled")
            ip3.configure(state="disabled")
            ip4.configure(state="disabled")
            port.configure(state="disabled")
            reciever_thread=th.Thread(target=reciever, args=())
            sender_thread=th.Thread(target=sender,args=())
            reciever_thread.start()
            sender_thread.start()
            popup.protocol("WM_DELETE_WINDOW",lambda: destroy(popup))
        else:
            connecting_label.configure(text="Connection Refused")
            skt.close()
            skt=socket.socket()
            popup.protocol("WM_DELETE_WINDOW",lambda: destroy(popup))
    except ConnectionRefusedError:
        connecting_label.configure(text="Connection Refused")
        skt.close()
        skt=socket.socket()
        popup.protocol("WM_DELETE_WINDOW",lambda: destroy(popup))
        return
    except TimeoutError:
        connecting_label.configure(text="Connection Timed Out")
        skt.close()
        skt=socket.socket()
        popup.protocol("WM_DELETE_WINDOW",lambda: destroy(popup))
        return
    
def acceptconn(popup_waitconnection):
    global connection_accepted,skt,sender_thread,reciever_thread,file_directory
    connection_accepted=True
    select.select([],[skt.fileno()],[])
    skt.sendall("accepted".encode())
    popup_waitconnection.destroy()
    popup_waitconnection.update()
    connect_start.configure(state="disabled")
    file_button.configure(state="normal")
    file_directory=str(os.path.abspath(os.getcwd()))
    filename_var.set("Location: "+file_directory)
    disconnect_button.configure(state="normal")
    dropdown.configure(state="disabled")
    ip1.configure(state="disabled")
    ip2.configure(state="disabled")
    ip3.configure(state="disabled")
    ip4.configure(state="disabled")
    port.configure(state="disabled")
    reciever_thread=th.Thread(target=reciever, args=())
    sender_thread=th.Thread(target=sender,args=())
    reciever_thread.start()
    sender_thread.start()
    return

def rejectconn(popup_waitconnection):
    global skt,serverskt
    popup_waitconnection.destroy()
    popup_waitconnection.update()
    try:
        skt.close()
    except AttributeError:
        pass
    serverskt.close()
    serverskt=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    return

def host_connection(pendingconnection_label,acceptconn_butt,rejectconn_butt):
    global serverskt,skt, incoming_address
    try:
        skt, incoming_addr=serverskt.accept()
    except OSError:
        return
    pendingconnection_label.configure(text='Accept Connection from'+str(incoming_addr).split(",")[0]+')?')
    incoming_address=str(incoming_addr).split(",")[0][1:][1:-1]
    acceptconn_butt.configure(state="normal")
    rejectconn_butt.configure(state="normal")
def connect():
    global port_var,base
    if not(int(port_var.get())>=2000 and int(port_var.get())<=49151):
        popup=tk.CTkToplevel(base)
        popup.resizable(False,False)
        popup.title("Connection")
        popup.geometry("+%d+%d" %(base.winfo_x(),base.winfo_y()))
        error_port_label=tk.CTkLabel(popup,text="Incorrect Port Number")
        error_port_label.pack()
        return
    if connect_start_var.get()=="Connect":
        popup_attemptconnection=tk.CTkToplevel(base)
        popup_attemptconnection.resizable(False,False)
        popup_attemptconnection.title("Connection")
        popup_attemptconnection.geometry("+%d+%d" %(base.winfo_x(),base.winfo_y()))
        popup_attemptconnection.maxsize(width=300,height=100)
        popup_attemptconnection.columnconfigure(index=0,minsize=300)
        popup_attemptconnection.rowconfigure(index=0,minsize=100)
        connecting_label=tk.CTkLabel(popup_attemptconnection,text="",justify='center')
        connecting_label.grid(row=0,column=0,stick="nsew")
        popup_attemptconnection.grab_set()
        client_connection_thread=th.Thread(target=client_connection,args=(connecting_label,popup_attemptconnection,))
        client_connection_thread.start()
        popup_attemptconnection.protocol("WM_DELETE_WINDOW",donothing)
    else:
        popup_waitconnection=tk.CTkToplevel(base)
        popup_waitconnection.resizable(False,False)
        popup_waitconnection.title("Connection")
        popup_waitconnection.geometry("+%d+%d" %(base.winfo_x(),base.winfo_y()))
        popup_waitconnection.minsize(width=300,height=100)
        popup_waitconnection.columnconfigure(0,minsize=150)
        popup_waitconnection.columnconfigure(1,minsize=150)
        popup_waitconnection.rowconfigure(0,minsize=50)
        popup_waitconnection.rowconfigure(1,minsize=50)
        pendingconnection_label=tk.CTkLabel(popup_waitconnection,text='Waiting for Connection',justify="center")
        pendingconnection_label.grid(row=0,column=0,columnspan=2,sticky='nsew')
        acceptconn_butt=tk.CTkButton(popup_waitconnection,text='Yes',command=lambda: acceptconn(popup_waitconnection),state='disabled')
        acceptconn_butt.grid(row=1,column=0,sticky='nsew',padx=10)
        rejectconn_butt=tk.CTkButton(popup_waitconnection,text='No',command=lambda: rejectconn(popup_waitconnection),state='disabled')
        rejectconn_butt.grid(row=1,column=1,sticky='nsew',padx=10)
        popup_waitconnection.protocol("WM_DELETE_WINDOW",lambda: rejectconn(popup_waitconnection))
        serverskt.bind((ip1_var.get()+'.'+ip2_var.get()+'.'+ip3_var.get()+'.'+ip4_var.get(),int(port_var.get())))
        serverskt.listen(1)
        popup_waitconnection.grab_set()
        server_connection_thread=th.Thread(target=host_connection,args=(pendingconnection_label,acceptconn_butt,rejectconn_butt))
        server_connection_thread.start()
connect_start.configure(command=connect)
def getfile():
    global sendfile_button,filename_var,file_directory,folder_sending
    if file_button.cget("text")=="Select File":
        temp_file_directory=askopenfilename()
        if temp_file_directory:
            sendfile_button.configure(state="normal")
            filename_var.set(temp_file_directory.split("/")[-1]+'\n('+str(os.path.getsize(temp_file_directory))+' Bytes)')
            file_directory=temp_file_directory
            folder_sending=False
    else:
        download_location=filedialog.askdirectory()
        if download_location:
            filename_var.set("Location: "+download_location)
            file_directory=download_location
file_button.configure(command=getfile)
def getfolder():
    global sendfile_button,filename_var,file_directory,folder_sending
    temp_file_directory=filedialog.askdirectory()
    if temp_file_directory:
        sendfile_button.configure(state="disabled")
        file_directory=temp_file_directory
        temp_size=0
        for path, dirs, files in os.walk(file_directory):
            for f in files:
                fp = os.path.join(path, f)
                temp_size += os.path.getsize(fp)
        filename_var.set(temp_file_directory.split("/")[-1]+'\n('+str(temp_size)+' Bytes)')
        sendfile_button.configure(state="normal")
        folder_sending=True
selectfolder_button.configure(command=getfolder)
def pausetransfer():
    global pause_file_send
    if pause_file_send.is_set():
        pause_file_send.clear()
    else:
        pause_file_send.set()
def stoptransfer():
    global stop_file_send
    stop_file_send.clear()
    pause_file_send.set()
def sendfile():
    global sendfile_button,base,dropdown_var,popup_file,pendingconfirmation_label,filename_var,file_send_progressbar,filename
    global percentage_label,stop_transfer_button,pause_transfer_button
    popup_file=tk.CTkToplevel(base)
    popup_file.resizable(False,False)
    popup_file.title("File/Folder Transfer")
    popup_file.geometry("+%d+%d" %(base.winfo_x(),base.winfo_y()))
    popup_file.grab_set()
    if dropdown_var.get()=="Client(Send File)":
        if folder_sending:
            sending_queue.put(("<<FOLDERREQUEST>>//"+filename_var.get()).encode())
        else:
            sending_queue.put(("<<FILEREQUEST>>//"+filename_var.get()).encode())
    popup_file.minsize(width=300,height=133)
    popup_file.columnconfigure(0,minsize=150)
    popup_file.columnconfigure(1,minsize=150)
    popup_file.rowconfigure(0,minsize=33)
    popup_file.rowconfigure(1,minsize=33)
    popup_file.rowconfigure(2,minsize=33)
    popup_file.rowconfigure(3,minsize=33)
    pendingconfirmation_label=tk.CTkLabel(popup_file,text='Waiting for host to accept.',justify='center',wraplength=300,width=300)
    pendingconfirmation_label.grid(row=0,column=0,columnspan=2)
    if dropdown_var.get()=="Host(Recieve File)":
        if folder_sending:
            pendingconfirmation_label.configure(text="Accept Folder(It will be created for you):\n  "+filename+"?")
        else:
            pendingconfirmation_label.configure(text="Accept File:  "+filename+"?")
    file_send_progressbar=tk.CTkProgressBar(popup_file, orientation="horizontal",width=250,mode="determinate")
    file_send_progressbar.set(0.0)
    file_send_progressbar.grid(row=1,column=0,columnspan=2)
    percentage_label=tk.CTkLabel(popup_file,text='0%',justify="center")
    percentage_label.grid(row=2,column=0,columnspan=2)
    if dropdown_var.get()=="Host(Recieve File)":
        rejectfile_butt=tk.CTkButton(popup_file,text='Reject',command=lambda: rejectfile(dropdown_var))
        rejectfile_butt.grid(row=3,column=1,sticky='nsew',padx=10)
        acceptfile_butt=tk.CTkButton(popup_file,text='Accept',command=lambda: acceptfile(acceptfile_butt,rejectfile_butt))
        acceptfile_butt.grid(row=3,column=0,sticky='nsew',padx=10)
        if shutil.disk_usage(file_directory)[2]<int(filename.split('\n')[1][1:-7]):
            acceptfile_butt.configure(state="disabled")
            if folder_sending:
                pendingconfirmation_label.configure(text="Accept Folder(It will be created for you):  "+filename+"?"+"(Not Enough Space in current directory)")
            else:
                pendingconfirmation_label.configure(text="Accept:  "+filename+"?"+"(Not Enough Space in current directory)")
    else:
        pause_transfer_button=tk.CTkButton(popup_file,text='Pause',command=lambda: pausetransfer(),state="disabled")
        pause_transfer_button.grid(row=3,column=0,sticky="nsew",padx=10)
        stop_transfer_button=tk.CTkButton(popup_file,text='Cancel',command=lambda: stoptransfer(),state="disabled")
        stop_transfer_button.grid(row=3,column=1,sticky="nsew",padx=10)
    popup_file.protocol("WM_DELETE_WINDOW",donothing)
    return
sendfile_button.configure(command=sendfile)
def acceptstream():
    global stream_popup,stream_label,stream_accept_button,stream_reject_button,streaming_var
    if dropdown_var.get()=="Client(Send File)":
        stream_label.configure(text="Host Accepted Stream")
        stop_stream_button.configure(state="normal")
        streaming_var=True
        stream_client=th.Thread(target=client_streaming,args=())
        stream_client.start()
    else:
        streaming_var=True
        sending_queue.put("<<ACCEPTSTREAM>>".encode())
        stream_label.configure(text="Stream Accepted(Close this window to end stream)")
        stream_popup.protocol("WM_DELETE_WINDOW",lambda: stop_stream())
        stream_host=th.Thread(target=host_streaming,args=())
        stream_accept_button.configure(state="disabled")
        stream_reject_button.configure(state="disabled")
        stream_host.start()
def rejectstream():
    global stream_popup,stream_label,stream_accept_button,stream_reject_button
    if dropdown_var.get()=="Client(Send File)":
        stream_label.configure(text="Host Rejected Stream")
        start_stream_button.configure(state="normal")
        video_stream_check.configure(state="normal")
        mic_stream_check.configure(state="normal")
        system_audio_stream_check.configure(state="normal")
        system_mouse_stream_check.configure(state="normal")
        system_keyboard_stream_check.configure(state="normal")
        stream_popup.protocol("WM_DELETE_WINDOW",lambda: destroy(stream_popup))
    else:
        sending_queue.put("<<REJECTSTREAM>>".encode())
        stream_popup.destroy()
        stream_popup.update()
def stop_stream():
    global streaming_var,video_stream_sender,mic_audio_stream_sender,video_stream_reciever,mic_audio_stream_reciever,system_audio_reciever
    global system_mouse_stream_var,system_keyboard_stream_var, system_mouse_stream_var_recv, system_keyboard_stream_var_recv
    global mouse_stream_listener, keyboard_stream_listener, mouse_stream_receive_socket, mouse_stream_receive_server, mouse_stream_sender_socket
    global keyboard_stream_receive_server, keyboard_stream_receive_socket, keyboard_stream_sender_socket
    if dropdown_var.get()=="Client(Send File)":
        if video_stream_var.get():
            video_stream_sender.stop_stream()
        if mic_stream_var.get():
            mic_audio_stream_sender.stop_stream()
        if system_audio_stream_var.get():
            stop_system_audio_event.set()
        if(system_mouse_stream_var.get()):
            system_mouse_stream_var.set(0)
        if(system_keyboard_stream_var.get()):
            system_keyboard_stream_var.set(0)
        stream_popup.protocol("WM_DELETE_WINDOW",lambda: destroy(stream_popup))
        start_stream_button.configure(state="normal")
        video_stream_check.configure(state="normal")
        mic_stream_check.configure(state="normal")
        system_audio_stream_check.configure(state="normal")
        system_mouse_stream_check.configure(state="normal")
        system_keyboard_stream_check.configure(state="normal")
        stop_stream_button.configure(state="disabled")
        if streaming_var:
            streaming_var=False
            sending_queue.put("<<ENDSTREAM>>".encode())
            stream_label.configure(text="Stream Stopped")
        else:
            stream_label.configure(text="Stream Stopped-Host closed the stream")
    else:
        file_status.configure(text="Stream Ended by Client")
        if streaming_var:
            streaming_var=False
            file_status.configure(text="Stream Ended")
            sending_queue.put("<<ENDSTREAM>>".encode())
        if video_stream_var_recv:
            video_stream_reciever.stop_server()
        if mic_stream_var_recv:
            mic_audio_stream_reciever.stop_server()
        if system_audio_stream_var_recv:
            system_audio_reciever.stop_server()
        if system_mouse_stream_var_recv:
            mouse_stream_listener.stop()
            mouse_stream_sender_socket.close()
        if system_keyboard_stream_var_recv:
            keyboard_stream_listener.stop()
            keyboard_stream_sender_socket.close()
        stream_popup.destroy()
        stream_popup.update()
def start_stream():
    global video_stream_var,mic_stream_var,system_audio_stream_var,video_stream_check,mic_stream_check,system_audio_stream_check, system_mouse_stream_check,system_keyboard_stream_check
    global system_mouse_stream_var,system_keyboard_stream_var
    stream_popup.protocol("WM_DELETE_WINDOW",donothing)
    stream_label.configure(text="Waiting for Host to accept Stream.")
    start_stream_button.configure(state="disabled")
    video_stream_check.configure(state="disabled")
    mic_stream_check.configure(state="disabled")
    system_audio_stream_check.configure(state="disabled")
    system_mouse_stream_check.configure(state="disabled")
    system_keyboard_stream_check.configure(state="disabled")
    str1="<<REQUESTSTREAM>>"
    if video_stream_var.get():
        str1+="<<V>>"
    if mic_stream_var.get():
        str1+="<<M>>"
    if system_mouse_stream_var.get():
        str1+="<<T>>"
    if system_keyboard_stream_var.get():
        str1+="<<K>>"
    if system_audio_stream_var.get():
        str1+="<<S>>"
        str1+="<<<"+str(default_speakers["defaultSampleRate"])+">>>"
    sending_queue.put(str1.encode())
def number_check(value):
    if str.isdigit(value) or value == "":
        return True
    else:
        return False
number_validate=base.register(number_check)
def streamcallback(*args):
    global video_stream_res_x_entry,video_stream_res_y_entry,video_stream_res_x_var,video_stream_res_y_var
    if mic_stream_var.get() or system_audio_stream_var.get() or video_stream_var.get():
        if (video_stream_var.get() and video_stream_res_y_var.get() and video_stream_res_x_var.get() and int(video_stream_res_y_var.get())>=720 and int(video_stream_res_y_var.get())<=max_y_res and int(video_stream_res_x_var.get())>=1024 and int(video_stream_res_x_var.get())<=max_x_res):
            start_stream_button.configure(state="normal")
        elif not video_stream_var.get():
            start_stream_button.configure(state="normal")
        else:
            start_stream_button.configure(state="disabled")
    else:
        start_stream_button.configure(state="disabled")
    if video_stream_var.get():
        video_stream_res_x_entry.grid(row=4,column=0)
        video_stream_res_y_entry.grid(row=4,column=1)
        video_stream_label.grid(row=4,column=2,rowspan=2)
        video_stream_res_x_label.grid(row=5,column=0)
        video_stream_res_y_label.grid(row=5,column=1)
    else:
        video_stream_res_x_entry.grid_forget()
        video_stream_res_y_entry.grid_forget()
        video_stream_label.grid_forget()
        video_stream_res_x_label.grid_forget()
        video_stream_res_y_label.grid_forget()
def stream(data=None):
    global base,stream_popup,stream_label,stream_accept_button,stream_reject_button,default_speakers,video_stream_var_recv,mic_stream_var_recv,system_audio_stream_var_recv
    global start_stream_button,stop_stream_button,video_stream_check,mic_stream_check,system_audio_stream_check,received_rate
    global video_stream_res_x_entry,video_stream_res_y_entry,video_stream_label,video_stream_res_x_label,video_stream_res_y_label
    global system_keyboard_stream_check,system_mouse_stream_check,system_keyboard_stream_var,system_mouse_stream_var
    global system_mouse_stream_var_recv,system_keyboard_stream_var_recv
    stream_popup=tk.CTkToplevel(base)
    stream_popup.resizable(False,False)
    stream_popup.title("Stream")
    stream_popup.grab_set()
    stream_popup.geometry("+%d+%d" %(base.winfo_x(),base.winfo_y()))
    stream_label=tk.CTkLabel(stream_popup,text="",justify="center")
    if dropdown_var.get()!="Client(Send File)":
        stream_popup.protocol("WM_DELETE_WINDOW",rejectstream)
        video_stream_var_recv=False
        mic_stream_var_recv=False
        system_audio_stream_var_recv=False
        stream_popup.maxsize(500,100)
        stream_popup.rowconfigure(0,minsize=50)
        stream_popup.rowconfigure(1,minsize=50)
        stream_popup.columnconfigure(0,minsize=150)
        stream_popup.columnconfigure(1,minsize=150)
        stream_label_string="Accept Stream(Contains:"
        tempvariable=19
        if data[tempvariable]=='V':
            stream_label_string+="Video"
            tempvariable+=5
            video_stream_var_recv=True
        if len(data)>tempvariable and data[tempvariable]=='M':
            stream_label_string+=",Mic Audio"
            tempvariable+=5
            mic_stream_var_recv=True
        if len(data)>tempvariable and data[tempvariable]=="T":
            stream_label_string+=",Control Mouse"
            tempvariable+=5
            system_mouse_stream_var_recv=True
        if len(data)>tempvariable and data[tempvariable]=="K":
            stream_label_string+=",Control Keyboard"
            tempvariable+=5
            system_keyboard_stream_var_recv=True
        if len(data)>tempvariable and data[tempvariable]=='S':
            stream_label_string+=",System Audio"
            tempvariable+=5
            system_audio_stream_var_recv=True
            received_rate=float(data[tempvariable+1:len(data)-3])
        stream_label_string+=")?"
        stream_label.configure(text=stream_label_string)
        stream_label.grid(row=0,column=0,columnspan=2,pady=10)
        stream_accept_button=tk.CTkButton(stream_popup,text="Accept",command=acceptstream)
        stream_accept_button.grid(row=1,column=0)
        stream_reject_button=tk.CTkButton(stream_popup,text="Reject",command=rejectstream)
        stream_reject_button.grid(row=1,column=1)
    else:
        stream_popup.minsize(300,250)
        stream_popup.rowconfigure(0,minsize=50)
        stream_popup.rowconfigure(1,minsize=50)
        stream_popup.rowconfigure(2,minsize=50)
        stream_popup.rowconfigure(3,minsize=50)
        stream_popup.rowconfigure(4,minsize=50)
        video_stream_res_x_entry=tk.CTkEntry(stream_popup,textvariable=video_stream_res_x_var,placeholder_text="X-Res",validate="all",validatecommand=(number_validate,'%P'))
        video_stream_res_y_entry=tk.CTkEntry(stream_popup,textvariable=video_stream_res_y_var,placeholder_text="Y-Res",validate="all",validatecommand=(number_validate,'%P'))
        video_stream_label=tk.CTkLabel(stream_popup,text="         Ranges\nX-Res (1024-"+str(max_x_res)+")\nY-Res (720-"+str(max_y_res)+")")
        video_stream_res_x_label=tk.CTkLabel(stream_popup,text="X-Res")
        video_stream_res_y_label=tk.CTkLabel(stream_popup,text="Y-Res")
        stream_popup.columnconfigure(0,minsize=100)
        stream_popup.columnconfigure(1,minsize=100)
        stream_popup.columnconfigure(2,minsize=100)
        # stream_popup.columnconfigure(3,minsize=100)
        # stream_popup.columnconfigure(4,minsize=100)
        stream_label.configure(text="Confirm Settings")
        start_stream_button=tk.CTkButton(stream_popup,text="Start Stream",command=start_stream,state='disabled')
        start_stream_button.grid(row=3,column=0,columnspan=1)
        stop_stream_button=tk.CTkButton(stream_popup,text="Stop Stream",command=stop_stream,state='disabled')
        stop_stream_button.grid(row=3,column=2,columnspan=1)
        video_stream_var.set(0)
        mic_stream_var.set(0)
        system_audio_stream_var.set(0)
        stream_label.grid(row=0,column=0,columnspan=3,pady=10)
        video_stream_check=tk.CTkCheckBox(stream_popup,variable=video_stream_var,text="Video")
        video_stream_check.grid(row=1,column=0)
        mic_stream_check=tk.CTkCheckBox(stream_popup,text="Microphone Audio",variable=mic_stream_var)
        mic_stream_check.grid(row=1,column=1)
        system_audio_stream_check=tk.CTkCheckBox(stream_popup,text="System Audio",variable=system_audio_stream_var,state="disabled")
        system_audio_stream_check.grid(row=1,column=2)
        system_keyboard_stream_check=tk.CTkCheckBox(stream_popup,text="Allow Keyboard Control",variable=system_keyboard_stream_var)
        system_keyboard_stream_check.grid(row=2,column=0)
        system_mouse_stream_check=tk.CTkCheckBox(stream_popup,text="Allow Mouse Control",variable=system_mouse_stream_var)
        system_mouse_stream_check.grid(row=2,column=2)
        try:
            wasapi_info = pyaudiowpatch.PyAudio().get_host_api_info_by_type(pyaudiowpatch.paWASAPI)
        except OSError:
            return
        default_speakers = pyaudiowpatch.PyAudio().get_device_info_by_index(wasapi_info["defaultOutputDevice"])
        if not default_speakers["isLoopbackDevice"]:
            for loopback in pyaudiowpatch.PyAudio().get_loopback_device_info_generator():
                if default_speakers["name"] in loopback["name"]:
                    default_speakers = loopback
                    system_audio_stream_check.configure(state="normal")
                    break
            else:
                return
stream_button.configure(command=stream)
video_stream_res_x_var.trace_add(mode="write",callback=streamcallback)
video_stream_res_y_var.trace_add(mode="write",callback=streamcallback)
video_stream_var.trace_add(mode="write",callback=streamcallback)
mic_stream_var.trace_add(mode="write",callback=streamcallback)
system_audio_stream_var.trace_add(mode="write",callback=streamcallback)
def disconnect():
    global serverskt,skt,start_disconnect
    connect_start.configure(state="normal")
    disconnect_button.configure(state="disabled")
    file_button.configure(state="disabled")
    selectfolder_button.configure(state="disabled")
    sendfile_button.configure(state="disabled")
    stream_button.configure(state="disabled")
    dropdown.configure(state="normal")
    filename_var.set("Disconnected")
    if dropdown_var.get()=='Client(Send File)':
        ip1.configure(state="normal")
        ip2.configure(state="normal")
        ip3.configure(state="normal")
        ip4.configure(state="normal")
        skt.close()
        skt=socket.socket()
    else:
        if not start_disconnect:
            start_disconnect=True
        else:
            start_disconnect=False
            skt.close()
            skt=socket.socket()
            serverskt.close()
            serverskt=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    port.configure(state="normal")
disconnect_button.configure(command=disconnect)
def endprogram():
    global base
    base.destroy()
    if skt:
        skt.close()
    if serverskt:
        serverskt.close()
    os._exit(1)
base.protocol("WM_DELETE_WINDOW",endprogram)
base.mainloop()