
#include <due_can.h>
#include "M2RET.h"
#include "bt_interface.h"
#include "Logger.h"
#include <MCP2515_sw_can.h>

extern SWcan SWCAN;

#define BTDEBUG

enum FrameType {
    FT_INVALID,
    FT_DATA,
    FT_EVENT,
    FT_REPLY,
    FT_TCODE,
    FT_OBD,
    FT_PTMSG
};

enum Event {
    EV_PWROFF,
    EV_PWRON,
    EV_DCOFF,
    EV_DCON,
    EV_CCUP,
    EV_CCDN,
    EV_CCPWR,
    EV_CCCANCEL,
    EV_VOLUP,
    EV_VOLDN,
    EV_TRACKUP,
    EV_TRACKDN,
    EV_SRC,
    EV_VOICE,
    EV_MUTE,
    EV_DCCMDOFF,
    EV_DCCMDON,
    EV_BUS_INACTIVE,
    EV_BUS_ACTIVE,
    EV_KEYOFF,
    EV_KEYON,
    EV_UNLOCK,
    EV_LOCK
};

enum EnableState {
    SEND_DISABLE,
    SEND_ENABLE,
    SEND_ONESHOT
};

enum Interface {
    INTERFACE_BT,
    INTERFACE_RPI,
    INTERFACE_USB,
    INTERFACE_MAX
};

enum DiagUser {
    DU_WINDOW_ROLL = 31
};

const short WAKEUP_FRAMES[] = {
    0x620,
    0x621,
    0x622,
    0x627,
    0x62C,
    0x62D,
    0
};

const uint8_t RN42XV_ESCAPE_CHAR_1 = 0x05;
const uint8_t RN42XV_ESCAPE_CHAR_2 = 0x04;
const uint8_t SOH = 1;
const uint8_t STX = 2;
const uint8_t ETX = 3;

const int DASHCAM_GPIO = GPIO1;

const int PID_MONITOR_STATUS    = 0x01;
const int PID_ENGINE_LOAD       = 0x04;
const int PID_COOLANT_TEMP      = 0x05;

const int PID_ENGINE_RPM        = 0x0C;
const int PID_VEHICLE_SPEED     = 0x0D;

const int PID_INTAKE_TEMP       = 0x0F;
const int PID_INTAKE_AIR_FLOW   = 0x10;
const int PID_THROTTLE_POSITION = 0x11;
const int PID_TIME_SINCE_START  = 0x1F;
const int PID_FUEL_LEVEL        = 0x2F;
const int PID_DIST_SINCE_CLEAR  = 0x31;
const int PID_AIR_TEMP          = 0x46;
const int PID_AIR_PRESSURE      = 0x33;

const int PID_BATTERY_RAW_SOC   = 0x5B;

// GM Hybrid/EV specific PIDs
// https://docs.google.com/spreadsheets/d/1HgWCnosdRqZYoWHEl7ylAxjxv4UFVeisWiR7gcr8H6I/edit#gid=0

#define MODULE_CODE(mod, code) ((1 + mod) << 16) | code;

const int PID_HV_AMPS           = MODULE_CODE(1, 0x2414);
const int PID_HV_VOLTS          = MODULE_CODE(1, 0x2429);
const int PID_MGA_AMPS          = MODULE_CODE(1, 0x2883);
const int PID_MGA_VOLTS         = MODULE_CODE(1, 0x2885);
const int PID_MGB_AMPS          = MODULE_CODE(1, 0x2884);
const int PID_MGB_VOLTS         = MODULE_CODE(1, 0x2886);

const int PID_BATTERY_TEMP      = MODULE_CODE(4, 0x434F);
const int PID_BATTERY_SOC       = MODULE_CODE(4, 0x8334);

const int PID_EV_KM             = 0x2487;
const int PID_GEAR_SELECTOR     = 0x2889;

const int PID_EV_RANGE_REM      = MODULE_CODE(4, 0x41A6);

// pseudo-pids
const int PID_TROUBLECODE_QUERY = 0xFFFE;
const int PID_NOTHING_TO_SEND   = 0xFFFF;

#include "obd2_query_sequence.h"

#ifdef BTDEBUG
#define D(a, ...) if (SysSettings.btDebugEnable) { Logger::console(a, ## __VA_ARGS__); } else (void)0
#else
#define D(a, ...) (void)0
#endif

#define GET8(frame, pos) (frame.data.bytes[pos])
#define GET16(frame, pos) ((frame.data.bytes[pos] << 8) | frame.data.bytes[pos+1])
#define GET24(frame, pos) ((frame.data.bytes[pos] << 16) | (frame.data.bytes[pos+1] << 8) | frame.data.bytes[pos+2])
#define GET32(frame, pos) ((frame.data.bytes[pos] << 24) | (frame.data.bytes[pos+1] << 16) | (frame.data.bytes[pos+2] << 8) | frame.data.bytes[pos+3])
#define WBITS(b, v) bs.wbits(b, b == 32 ? v : v & ((1 << b) - 1), #v)
#define XWBITS(b, v, d) bs.wbits(b, b == 32 ? v : v & ((1 << b) - 1), d)

#define BUFFER_LEN 128

// from http://stackoverflow.com/questions/10564491/function-to-calculate-a-crc16-checksum
static uint16_t crc16(const uint8_t* data_p, int length) {
    uint8_t x;
    uint16_t crc = 0xFFFF;

    while (length--){
        x = crc >> 8 ^ *data_p++;
        x ^= x>>4;
        crc = (crc << 8) ^ ((uint16_t)(x << 12)) ^ ((uint16_t)(x <<5)) ^ ((uint16_t)x);
    }
    return crc;
}

enum BluetoothState {
    BLUETOOTH_RESET,      // bt module is in reset and can be powered on
    BLUETOOTH_HOLDRESET,  // has just been reset; need to hold it in reset for a bit
    BLUETOOTH_WAITRESET,  // about to be disabled; wait to reset to allow it to send clean disconnect notification
    BLUETOOTH_BOOTING,    // bt module is booting after reset
    BLUETOOTH_CONNECTING, // bt module is trying to connect
    BLUETOOTH_CONNECTED,  // bt module is connected
    BLUETOOTH_WAITRETRY,  // connection failed, waiting to retry
    BLUETOOTH_POWERON,    // manually powered on for passthrough
};

enum InputState {
    IS_IDLE,
    IS_QUERY,
    IS_EVENT1,
    IS_EVENT2
};

enum InputReturn {
    IR_NOACTION,
    IR_CONNECT,
    IR_DISCONNECT,
    IR_REBOOT,
    IR_QUERY
};

static const uint32_t SEND_INTERVAL = 50;
static const uint32_t SEND_INTERVAL_LOW = 200;

struct Clock {
    uint32_t fw_millis;
    uint8_t year, month, day, hour, min, sec;
};

struct CarData {
    uint32_t last_send_millis;

    //AUTO START : struct CarData
    uint32_t wrc;
    uint32_t fuel_ctr;
    uint32_t odometer;
    uint32_t scflags;
    uint32_t lat;
    uint32_t lon;
    uint16_t wrc1;
    uint16_t wrc2;
    uint16_t mga_rpm;
    uint16_t mgb_rpm;
    uint16_t speed;
    uint16_t hv_amps;
    uint16_t mga_amps;
    uint16_t mgb_amps;
    uint16_t hv_volts;
    uint16_t mga_volts;
    uint16_t mgb_volts;
    uint16_t steer;
    uint16_t engine_rpm;
    uint16_t ev_range_rem;
    uint16_t ccspeed;
    uint8_t brake;
    uint8_t accel;
    uint8_t climate_power;
    uint8_t climate_mode;
    uint8_t heat_ac;
    uint8_t battery_raw_soc;
    uint8_t battery_soc;
    uint8_t clutch_state;
    uint8_t ccbtn;
    uint8_t radiobtn;
    uint8_t coolant_temp;
    uint8_t intake_temp;
    uint8_t battery_temp;
    uint8_t air_temp1;
    uint8_t air_temp2;
    uint8_t air_pressure;
    uint8_t tire_ft_lf;
    uint8_t tire_rr_lf;
    uint8_t tire_ft_rt;
    uint8_t tire_rr_rt;
    uint8_t oil_life;
    uint8_t fanspeed;
    uint8_t vent;
    uint8_t select_fanspeed;
    uint8_t select_temp;
    uint8_t recirc;
    uint8_t gear;
    uint8_t drive_mode;
    uint8_t rear_defrost;
    //AUTO END

    uint8_t frameseq;
    uint8_t enable_send;
    bool want_full_update;

    bool should_send(uint32_t msnow, uint32_t send_interval) {
        if (enable_send != SEND_DISABLE) {
            uint32_t timediff = msnow - last_send_millis;
            if (timediff > send_interval) {
                last_send_millis += send_interval;
                timediff = msnow - last_send_millis;
                if (timediff > send_interval) {
                    last_send_millis = msnow;
                }
                if (enable_send == SEND_ONESHOT)
                    enable_send = SEND_DISABLE;
                return true;
            }
        }
        return false;
    }
};

class InputStateMachine {
public:

    static const int MAX_QUERY = 127;
    InputReturn check_input(uint8_t ch, bool enable_pt);
    virtual void send_passthrough()=0;

    InputState input_state;
    uint8_t query_pos;
    char query[MAX_QUERY + 1];
};

class BTInputStateMachine : public InputStateMachine {
    virtual void send_passthrough() {
#ifdef DEBUG_RPI_PASSTHROUGH
        if (SysSettings.btDebugEnable) {
            for (int x = 0; x < query_pos; x++ ) {
                char ch = query[x];
                if (32 < ch && ch < 127) {
                    D("M->R: %c", ch);
                } else {
                    D("M->R: 0x%x", ch);
                }
            }
        }
#endif
        Serial3.write(query, query_pos);
    }
};

class CommandButtonManager {
protected:
    virtual void build_frame(CAN_FRAME& sframe, int button)=0;
public:
    CommandButtonManager() : last_button_send(0) { }

    void send(int button);
    void check_release(uint32_t msnow);

private:
    uint32_t last_button_send;
};

class ClimateButtonManager : public CommandButtonManager {
    virtual void build_frame(CAN_FRAME& sframe, int button) {
        sframe.id = 0x10AD6080;
        sframe.length = 8;
        sframe.data.bytes[1] = 7;
        sframe.data.bytes[2] = button == -1 ? 8 : button;
        sframe.data.bytes[3] = 2;
    }
};

class RadioButtonManager : public CommandButtonManager {
    virtual void build_frame(CAN_FRAME& sframe, int button) {
        sframe.id = 0x10438040;
        sframe.length = 1;
        sframe.data.bytes[0] = button == -1 ? 0 : button;
    }
};

struct WindowRollTimer {
    uint32_t last_send;
    uint16_t duration;

    void send(uint8_t cmd, uint8_t mask, uint16_t duration);
    void check_release(uint32_t msnow);
};

class RPIInputStateMachine : public InputStateMachine {
    virtual void send_passthrough() {
#ifdef DEBUG_RPI_PASSTHROUGH
        if (SysSettings.btDebugEnable) {
            for (int x = 0; x < query_pos; x++ ) {
                char ch = query[x];
                if (32 < ch && ch < 127) {
                    D("M->P: %c", ch);
                } else {
                    D("M->P: 0x%x", ch);
                }
            }
        }
#endif
        Serial.write(query, query_pos);
    }
};


static inline uint16_t pack_15(uint16_t val) {
    static const uint16_t hb_lookup[] = {
        0x4040, 0x4080, 0x40C0, 0x8040,
        0x8080, 0x80C0, 0xC040, 0xC080
    };
    return (val & 0x3F3F) | hb_lookup[((val >> 12) & 4) | ((val >> 6) & 3)];
}

static inline uint16_t unpack_15(uint16_t val) {
    uint16_t hb = (((val >> 14) & 3) * 3 + ((val >> 6) & 3)) - 4;
    return (val & 0x3F3F) | ((hb & 4) << 12) | ((hb & 3) << 6);
}

template<int nbits>
inline uint32_t rollover_bits(uint32_t oldval, uint32_t newval) {
    static const uint32_t inc = 1 << nbits;
    static const uint32_t mask = inc - 1;

    newval &= mask;
    if (newval < (oldval & mask)) {
        oldval += inc;
    }
    return (oldval & ~mask) | newval;

}

class BitStream {
public:
    bool wbits(int nbits, uint32_t v, const char* desc) {
        if (serial_enable) {
            Logger::console("%s = %d", desc, v);
        }
        //D("wbits(%x, %d) wpos=%d bpos=%d", v, nbits, buffer_wordpos, buffer_bitpos);
        bit_buffer[buffer_wordpos] |= (v << buffer_bitpos) & 0x7FFF;
        int bits_copied = 15 - buffer_bitpos;
        buffer_bitpos += nbits;
        if (buffer_bitpos >= 15) {

            v >>= bits_copied;
            nbits -= bits_copied;

            do
            {
                if (++buffer_wordpos == BUFFER_LEN)
                    return false;
                bit_buffer[buffer_wordpos] = v & 0x7FFF;
                v >>= 15;
                nbits -= 15;
            } while (nbits >= 0);

            buffer_bitpos = nbits + 15;
        }
        //D("... wpos=%d bpos=%d", buffer_wordpos, buffer_bitpos);

        return true;
    }

    void begin_frame() {
        buffer_wordpos = 1;
        buffer_bitpos = 0;
        bit_buffer[1] = 0;
    }

    void close_frame() {
        if (buffer_bitpos)
            buffer_wordpos++;

        while (buffer_wordpos > 3 && bit_buffer[buffer_wordpos - 1] == 0) buffer_wordpos -= 1;

        bit_buffer[0] = crc16((uint8_t*)&bit_buffer[1], (buffer_wordpos - 1) * 2) & 0x7FFF;
        for(int j = 0; j < buffer_wordpos; j++) {
            bit_buffer[j] = pack_15(bit_buffer[j]);
        }
    }

    bool serial_enable;
    uint16_t bit_buffer[BUFFER_LEN];
    uint8_t buffer_bitpos, buffer_wordpos;
};

struct obd_query {
    int pid;
    int whichmod;
    uint32_t send_time;
    uint8_t responded_mods;
};

union queue_buf {
    uint64_t value;
    uint8_t data[8];
};

class DiagQueue {
public:
    uint32_t last_alive_send, last_send, diag_inuse;
    uint16_t frameid;
    queue_buf diag_queue[16];
    uint8_t queue_head, queue_tail;

    DiagQueue() {
        frameid = 0;
        last_alive_send = 0;
        last_send = 0;
        queue_head = queue_tail = 0;
        diag_inuse = 0;
        memset(&diag_queue, 0, sizeof(diag_queue));
    }

    bool enqueue_frame(uint8_t d0=0, uint8_t d1=0, uint8_t d2=0, uint8_t d3=0, uint8_t d4=0, uint8_t d5=0, uint8_t d6=0, uint8_t d7=0) {
        uint8_t new_head = (queue_head + 1) & 15;
        if (new_head == queue_tail) {
            D("QUEUE FULL");
            return false;
        }

        D("enqueue(%d) %x [%x %x %x %x %x %x %x %x]", queue_head, frameid, d0, d1, d2, d3, d4, d5, d6, d7);
        queue_buf& frame(diag_queue[queue_head]);

        frame.data[0] = d0;
        frame.data[1] = d1;
        frame.data[2] = d2;
        frame.data[3] = d3;
        frame.data[4] = d4;
        frame.data[5] = d5;
        frame.data[6] = d6;
        frame.data[7] = d7;

        queue_head = new_head;
        return true;
    }

    bool enqueue_diag(uint8_t d2=0, uint8_t d3=0, uint8_t d4=0, uint8_t d5=0, uint8_t d6=0, uint8_t d7=0) {
        return enqueue_frame(0x07, 0xAE, d2, d3, d4, d5, d6, d7);
    }

    void send_next_diag_frame(uint32_t msnow) {
        if (queue_tail != queue_head) {
            last_send = millis();
            queue_buf& qf(diag_queue[queue_tail]);
            CAN_FRAME f;
            f.id = frameid;
            f.extended = false;
            f.rtr = 0;
            f.length = 8;
            f.data.value = qf.value;

            D("dequeue(%d) %x [%x %x %x %x %x %x %x %x]", queue_tail, f.id, f.data.bytes[0], f.data.bytes[1], f.data.bytes[2], f.data.bytes[3], f.data.bytes[4], f.data.bytes[5], f.data.bytes[6], f.data.bytes[7]);
            Can0.sendFrame(f);
            queue_tail = (queue_tail + 1) & 15;
        }
    }

    void check_send(uint32_t msnow) {
        if (diag_inuse != 0 && (msnow - last_alive_send) > 2000) {
            last_alive_send = msnow;
            enqueue_frame(0x01, 0x3E);
        }
        if ((msnow - last_send) > 200) {
            send_next_diag_frame(msnow);
        }
    }

    void diag_enable(int user, bool enable) {
        uint32_t bit = 1 << user;
        if (enable) {
            if (!diag_inuse)
                last_alive_send = millis();
            diag_inuse |= bit;
        } else {
            diag_inuse &= ~bit;
        }
    }
};

class BTInterface {
public:
    BTInterface();
    void gotframe(CAN_FRAME &frame, int busnum);
    void inputchar(uint8_t ch);
    void rpi_inputchar(uint8_t ch);
    void do_query(const char* query, Interface who);
    bool do_query_usb(const char* query);
    void loop();
    void reset();
    void check_power_on();
    void handle_obd2_reply(CAN_FRAME &frame);

    void send_obd2_query(int whichmod, int pid);

    void reset_bluetooth();

    inline bool xmit_enable() { return xmit_override || vehicle_power_on; }

    inline void begin_frame(FrameType ft) {
        bs.begin_frame();

        XWBITS(30, millis(), "time");
        XWBITS(3, ft, "frame_type");
    }

    inline void begin_reply_frame(uint8_t ch) {
        begin_frame(FT_REPLY);
        XWBITS(7, ch, "reply_type");
    }

    inline void send_event(Event evt) {
        D("send event: %d", evt);
        begin_frame(FT_EVENT);
        WBITS(6, evt);
        bs.close_frame();
        send_frame_bt();
        send_frame_rpi();
    }

    bool send_frame(Interface which) {
        switch (which) {
            case INTERFACE_BT:
                return send_frame_bt();
            case INTERFACE_RPI:
                return send_frame_rpi();
        }
        return false;
    }

    bool send_frame_bt() {
        if (bt_state != BLUETOOTH_CONNECTED)
            return false;

        Serial.write(STX);
        Serial.write(STX);
        Serial.write(STX);

        for(int j = 0; j < bs.buffer_wordpos; j++) {
            uint16_t val = bs.bit_buffer[j];
            Serial.write(val & 0xFF);
            Serial.write((val >> 8) & 0xFF);
        }
        Serial.write(ETX);
        Serial.write(ETX);

        return true;
    };

    bool send_frame_rpi() {
        Serial3.write(STX);
        for(int j = 0; j < bs.buffer_wordpos; j++) {
            uint16_t val = bs.bit_buffer[j];
            Serial3.write(val & 0xFF);
            Serial3.write((val >> 8) & 0xFF);
        }
        Serial3.write('\n');
        return true;
    }

    void build_data_frame(CarData& lcd) {
        begin_frame(FT_DATA);

        bool full_update = lcd.want_full_update;
        lcd.want_full_update = false;

        WBITS(1, full_update);
        if (full_update) {
            lcd.frameseq = 0;
        } else {
            WBITS(4, lcd.frameseq);
            lcd.frameseq++;
        }

#define PV(b, v)                                \
        if (full_update) {                      \
            lcd.v = cd.v;                       \
            XWBITS(b, cd.v, #v);                \
        } else if (lcd.v != cd.v) {             \
            lcd.v = cd.v;                       \
            XWBITS(1, 1, "p-" #v);              \
            XWBITS(b, cd.v, #v);                \
        } else {                                \
            XWBITS(1, 0, "p-" #v);              \
        }

        //AUTO START : build_data_frame
        PV(25, wrc);
        PV(13, wrc1);
        PV(13, wrc2);
        PV(15, mga_rpm);
        PV(15, mgb_rpm);
        PV(14, speed);
        PV(16, hv_amps);
        PV(16, mga_amps);
        PV(16, mgb_amps);
        PV(16, hv_volts);
        PV(16, mga_volts);
        PV(16, mgb_volts);
        PV(16, steer);
        PV(8, brake);
        PV(8, accel);
        PV(14, engine_rpm);
        PV(21, fuel_ctr);
        PV(7, climate_power);
        PV(2, climate_mode);
        PV(2, heat_ac);
        PV(8, battery_raw_soc);
        PV(8, battery_soc);
        PV(25, odometer);
        PV(16, ev_range_rem);
        PV(24, scflags);
        PV(8, clutch_state);
        PV(13, ccspeed);
        PV(4, ccbtn);
        PV(4, radiobtn);
        PV(8, coolant_temp);
        PV(8, intake_temp);
        PV(8, battery_temp);
        PV(31, lat);
        PV(31, lon);
        PV(8, air_temp1);
        PV(8, air_temp2);
        PV(8, air_pressure);
        PV(8, tire_ft_lf);
        PV(8, tire_rr_lf);
        PV(8, tire_ft_rt);
        PV(8, tire_rr_rt);
        PV(8, oil_life);
        PV(8, fanspeed);
        PV(3, vent);
        PV(5, select_fanspeed);
        PV(6, select_temp);
        PV(2, recirc);
        PV(3, gear);
        PV(2, drive_mode);
        PV(1, rear_defrost);
        //AUTO END

        bs.close_frame();
    }

    void set_dashcam_recording(bool val) {
        if (val != dcrecording) {
            dcrecording = val;
            D("dcrecording = %d", dcrecording);
            if (val) {
                camera_on_millis = millis();
            }

            send_event(val ? EV_DCON : EV_DCOFF);
        }
    }

    void set_dashcam_command(bool val) {
        if (val != dcenable) {
            dcenable = val;
            if (val) {
                D("dashcam command on");
#ifdef DIRECT_DASHCAM
                set_dashcam_recording(true);
                digitalWrite(DASHCAM_GPIO, HIGH);
#else
                send_event(EV_DCCMDON);
#endif
            } else {
                D("dashcam command off");
#ifdef DIRECT_DASHCAM
                set_dashcam_recording(false);
                digitalWrite(DASHCAM_GPIO, LOW);
#else
                send_event(EV_DCCMDOFF);
#endif
            }
        }
    }

    CarData cd, bt_lcd, rpi_lcd;

    BitStream bs;

    DiagQueue diag[3];

    BluetoothState bt_state;
    BTInputStateMachine bt_ism;
    RPIInputStateMachine rpi_ism;

    ClimateButtonManager climate_button;
    RadioButtonManager radio_button;

    WindowRollTimer wrt;

    Clock clock, clock_po;

    uint32_t last_send_millis;
    uint32_t last_periodic_millis;
    uint32_t last_frame_recv; // Time of last "active" frame received

    uint8_t periodic_cycle;

    uint32_t camera_on_millis;
    uint32_t vehicle_power_change_time;

    uint32_t odometer_po;

    bool fake_engine;

    uint16_t wrc_ignoreval;

    uint8_t lastccbtn;
    uint8_t lastradiobtn;

    uint8_t cur_obd_pid;


    // Whether or not we have commanded dashcam to start recording
    bool dcenable;

    // Whether or not dashcam is actually in recording state
    bool dcrecording;

    uint8_t last_dc_profile_index;

    bool xmit_override;
    bool bt_power_override;

    bool powertrain_enable;
    bool vehicle_power_on;
    bool key_on;
    bool bus_active;

    bool cpu_throttle;

    obd_query obd_queries[INTERFACE_MAX];
    uint8_t boot_command_attempt_count;

    bool log_unusual_replies;
};

BTInterface bti;

BTInterface::BTInterface() {
    dcenable = false;
    dcrecording = false;
    cpu_throttle = false;
    fake_engine = false;
    bus_active = false;
    key_on = false;
    cur_obd_pid = 0;

    log_unusual_replies = false;

    diag[0].frameid = 0x241;
    diag[1].frameid = 0x7E1;
    diag[2].frameid = 0x7E4;

    for (int i = 0; i < INTERFACE_MAX; i++) {
        obd_queries[i].pid = -1;
    }
    last_dc_profile_index = -1;
    memset(&cd, 0, sizeof(CarData));
    memset(&bt_lcd, 0, sizeof(CarData));
    memset(&rpi_lcd, 0, sizeof(CarData));

    odometer_po = 0;
    clock_po.year = 0;
    wrc_ignoreval = 0xFFFF;

    reset();
}

void bt_setup() {
    pinMode(Button1, INPUT);
    pinMode(Button2, INPUT);

#ifdef DIRECT_DASHCAM
    pinMode(DASHCAM_GPIO, OUTPUT);
    digitalWrite(DASHCAM_GPIO, LOW);

    pinMode(I_SENSE_EN, OUTPUT);
    digitalWrite(I_SENSE_EN, HIGH);
#endif

    pinMode(XBEE_RST, OUTPUT);
    digitalWrite(XBEE_RST, LOW);
}

void bt_loop() {
    bti.loop();
}

void bt_sendframe(CAN_FRAME &frame, int busnum) {
    bti.gotframe(frame, busnum);
}

void bt_inputchar(uint8_t ch) {
    bti.inputchar(ch);
}

void bt_u3inputchar(uint8_t ch) {
    bti.rpi_inputchar(ch);
}

bool bt_command(const char* cmd) {
    return bti.do_query_usb(cmd);
}

void BTInterface::reset() {
    bt_lcd.enable_send = SEND_DISABLE;
    rpi_lcd.enable_send = SEND_ENABLE;

    bt_lcd.want_full_update = true;
    rpi_lcd.want_full_update = true;
    cd.wrc = 0;
    cd.fuel_ctr = 0;
    lastccbtn = 0;
    lastradiobtn = 0;

    periodic_cycle = 0;

}

static uint32_t decode_hex(const char*& txt, int fixlen) {
    uint32_t rv = 0;
    for (int i = 0; i < fixlen; i++, txt++) {
        char ch = *txt;
        rv <<= 4;
        if (ch >= '0' && ch <= '9') {
            rv += ch - '0';
        } else if (ch >= 'a' && ch <= 'f') {
            rv += ch - 'a' + 10;
        } else if (ch >= 'A' && ch <= 'F') {
            rv += ch - 'A' + 10;
        } else {
            return rv;
        }
    }
    return rv;
}

static void send_wakeup_frame(int frameid) {
    CAN_FRAME frame;

    frame.id = frameid;
    frame.extended = false;
    frame.rtr = 0;

    //D("wakeup %x", frame.id);
    frame.length = 8;
    frame.data.bytes[0] = 1;
    frame.data.bytes[1] = 0xFF;
    frame.data.bytes[2] = 0xFF;
    frame.data.bytes[3] = 0xFF;
    frame.data.bytes[4] = 0xFF;
    frame.data.bytes[5] = 0xFF;
    frame.data.bytes[6] = 0;
    frame.data.bytes[7] = 00;
    noInterrupts();
    SWCAN.sendFrame(frame);
    interrupts();
}

void BTInterface::do_query(const char* query, Interface who) {
#define SEND() bs.close_frame(); send_frame(who)
#define CCD() (who == 0 ? bt_lcd : rpi_lcd)

    D("do_query(%d): [%s]", who, query);
    switch (query[0]) {
        case 'C':
        {
            if (query[1] == 'S' && query[2] == '=' && strlen(query) == 6) {
                int record_active = strcmp(&query[3], "REC") == 0;
                set_dashcam_recording(record_active);
            } else {
                begin_reply_frame('C');
                XWBITS(32, dcrecording ? millis() - camera_on_millis : 0xFFFFFFFF, "millis");
                SEND();
            }
        }
        break;
        case 'Q':
        {
            begin_reply_frame('Q');
            SEND();
        }
        break;
        case 'E':
        {
            uint8_t newstate = query[1] == 's' ? SEND_ONESHOT : SEND_ENABLE;
            begin_reply_frame('E');
            SEND();
            CCD().enable_send = newstate;
        }
        break;
        case 'e':
        {
            begin_reply_frame('e');
            SEND();
            CCD().enable_send = SEND_DISABLE;
        }
        break;
        case 'F':
            CCD().want_full_update = true;
            break;
        case 'D':
        {
            begin_reply_frame('D');
            int new_profile = 0;
            if (query[1] == 'R' or query[1] == 'r') {
                set_dashcam_command(query[1] == 'R');
                XWBITS(1, 1, "ok");
                SEND();
            } else {
                for (int i = 0; i < 3; i++) {
                    if (query[i + 1] < '0' || query[i + 1] > '3') {
                        XWBITS(1, 0, "ok");
                        SEND();
                        return;
                    }
                    new_profile |= (query[i + 1] - '0') << (i * 2);
                }
                XWBITS(1, 1, "ok");
                SEND();
                last_dc_profile_index = -1;
                settings.dashcamProfile = new_profile;
                saveSettings();
            }
        }
        break;
        case 'g':
        case 'G':
        {
            begin_reply_frame(query[0]);
            const char* qpos = query + 1;
            int whichmod = decode_hex(qpos, 1);
            if (whichmod >= 3) {
                XWBITS(1, 0, "ok");
                SEND();
                return;
            }

            int whichbit = decode_hex(qpos, 2);
            if (whichbit >= 32) {
                XWBITS(1, 0, "ok");
                SEND();
                return;
            }
            XWBITS(1, 1, "ok");
            SEND();
            diag[whichmod].diag_enable(whichbit, query[0] == 'G');
        } break;
        case 'X':
            begin_reply_frame('X');
            SEND();
            xmit_override = true;
            break;

        case 'x':
            begin_reply_frame('x');
            SEND();
            xmit_override = false;
            break;

        case 'A':
        {
            begin_reply_frame('A');
            SEND();
            int btn = atoi(&query[1]);
            climate_button.send(btn);
        } break;

        case 'R':
        {
            begin_reply_frame('R');
            SEND();
            int btn = atoi(&query[1]);
            radio_button.send(btn);
        } break;

        case '?': // reduced-size data frame
        {
            begin_reply_frame('?');
            WBITS(4, cd.ccbtn);
            WBITS(4, cd.radiobtn);
            WBITS(8, cd.fanspeed);
            WBITS(3, cd.vent);
            WBITS(5, cd.select_fanspeed);
            WBITS(6, cd.select_temp);
            WBITS(2, cd.recirc);
            WBITS(2, cd.climate_mode);
            WBITS(7, cd.climate_power);
            WBITS(2, cd.heat_ac);
            WBITS(1, cd.rear_defrost);

            SEND();

        }
        break;
        case 'T':
        {
            begin_reply_frame('T');
            XWBITS(32, (millis() - clock.fw_millis), "millis");
            WBITS(7, clock.year);
            WBITS(4, clock.month);
            WBITS(5, clock.day);
            WBITS(5, clock.hour);
            WBITS(6, clock.min);
            WBITS(6, clock.sec);
            SEND();
        }
        break;
        case 'S':
        {
            CAN_FRAME sframe;
            const char* qpos = query + 1;
            int whichbus = decode_hex(qpos, 1);
            sframe.id = decode_hex(qpos, 8);
            sframe.extended = sframe.id > 0x7FF;
            sframe.rtr = 0;
            sframe.length = decode_hex(qpos, 1);
            for (int i = 0; i < sframe.length; i++) {
                sframe.data.bytes[i] = decode_hex(qpos, 2);
            }
            begin_reply_frame('S');
            SEND();
            if (whichbus == 0) {
                Can0.sendFrame(sframe);
            } else if (whichbus == 1) {
                Can1.sendFrame(sframe);
            } else if (whichbus == 2) {
                noInterrupts();
                SWCAN.sendFrame(sframe);
                interrupts();
            }
        } break;
        case 'd':
        {
            const char* qpos = query + 1;
            int which = decode_hex(qpos, 1);
            if (which >= 3) {
                XWBITS(1, 0, "ok");
                SEND();
                return;
            }
            uint8_t d0 = decode_hex(qpos, 2);
            uint8_t d1 = decode_hex(qpos, 2);
            uint8_t d2 = decode_hex(qpos, 2);
            uint8_t d3 = decode_hex(qpos, 2);
            uint8_t d4 = decode_hex(qpos, 2);
            uint8_t d5 = decode_hex(qpos, 2);
            uint8_t d6 = decode_hex(qpos, 2);
            uint8_t d7 = decode_hex(qpos, 2);
            bool ok = diag[which].enqueue_frame(d0, d1, d2, d3, d4, d5, d6, d7);
            begin_reply_frame('d');
            WBITS(1, ok);
            SEND();
        } break;
        case 'W':
        {
            D("SWCAN Wakeup");
            begin_reply_frame('W');
            XWBITS(1, 1, "ok");
            SEND();

            noInterrupts();
            SWCAN.mode(1);
            interrupts();

            CAN_FRAME frame;

            frame.id = 0x100;
            frame.extended = false;
            frame.rtr = 0;
            frame.length = 0;
            noInterrupts();
            SWCAN.sendFrame(frame);
            interrupts();

            delay(5);

            if (!query[1]) {
                const short* cwf = WAKEUP_FRAMES;
                while (1) {
                    int id = *cwf++;
                    if (!id)
                        break;
                    send_wakeup_frame(id);
                    delay(1);
                }
            } else {
                const char* qpos = query + 1;
                const char* lpos = qpos;
                while (1) {
                    uint32_t cwf = decode_hex(qpos, 4);
                    if (qpos - lpos != 4)
                        break;
                    lpos = qpos;
                    send_wakeup_frame(cwf);
                    delay(1);
                }
            }
            noInterrupts();
            SWCAN.mode(3);
            interrupts();
        }
        break;
        case 'O':
        {
            begin_reply_frame('O');

            const char* qpos = query + 1;
            int whichmod = -1;
            if (*qpos == 'x') {
                qpos++;
            } else {
                whichmod = decode_hex(qpos, 1);
            }

            int nquery = *qpos == 't' ? PID_TROUBLECODE_QUERY : decode_hex(qpos, 4);

            XWBITS(1, 1, "ok");
            SEND();

            D("Sending OBD2 query: %x", nquery);
            obd_query& obd(obd_queries[who]);
            obd.pid = nquery;
            obd.send_time = millis();
            obd.responded_mods = 0;
            obd.whichmod = whichmod;
            send_obd2_query(whichmod, obd.pid);
        }
        break;
        case 'w':
        {
            const char* qpos = query + 1;
            uint8_t cmd = decode_hex(qpos, 1);
            uint8_t mask = decode_hex(qpos, 1);
            uint16_t duration = decode_hex(qpos, 4);
            wrt.send(cmd, mask, duration);
        } break;
        case 'K':
        {
            begin_reply_frame('K');
            WBITS(1, vehicle_power_on ? 1 : 0);
            WBITS(1, key_on ? 1 : 0);
            WBITS(1, bus_active ? 1 : 0);
            SEND();
        }
        break;
        case 'I':
        {
            CCD().enable_send = SEND_ENABLE;
            begin_reply_frame('I');
            XWBITS(32, dcrecording ? millis() - camera_on_millis : 0xFFFFFFFF, "millis");
            WBITS(7, clock_po.year);
            WBITS(4, clock_po.month);
            WBITS(5, clock_po.day);
            WBITS(5, clock_po.hour);
            WBITS(6, clock_po.min);
            WBITS(6, clock_po.sec);
            WBITS(32, clock_po.fw_millis);
            WBITS(25, odometer_po);
            SEND();
        }
        case 'M':
        {
            begin_frame(FT_PTMSG);
            const char* q = &query[1];
            while (*q) {
                WBITS(7, *q);
                q++;
            }
            bs.close_frame();
            if (who == INTERFACE_BT) send_frame_rpi(); else send_frame_bt();
        }
        break;
    }
#undef SEND
}

bool BTInterface::do_query_usb(const char* query) {
    if (query[0] == '.') {
        char ch = query[1];
        switch (ch) {
            case 'X':
            case 'x':
                xmit_override = ch == 'X';
                Logger::console("xmit_override = %d", xmit_override);
                return true;

            case 'B':
            case 'b':
                bt_power_override = ch == 'B';
                Logger::console("bt_power_override = %d", bt_power_override);
                return true;

            case 'D':
            case 'd':
                SysSettings.btDebugEnable = ch == 'D';
                Logger::console("Debug output = %d", SysSettings.btDebugEnable);
                return true;

            case 'U':
            case 'u':
                log_unusual_replies = ch == 'U';
                Logger::console("Unusual reply logging = %d", log_unusual_replies);
                return true;

            case 'C':
            case 'c':
                set_dashcam_command(ch == 'C');
                Logger::console("Dashcam override = %d", dcenable);
                return true;

            case 'F':
            case 'f':
                fake_engine = ch == 'F';
                Logger::console("Fake engine = %d", fake_engine);
                return true;
            case 'A':
            {
                int btn = atoi(&query[2]);
                Logger::console("Sending climate button: %d", btn);
                climate_button.send(btn);
            } break;

            case 'R':
            {
                int btn = atoi(&query[2]);
                Logger::console("Sending radio button: %d", btn);
                radio_button.send(btn);
            } break;

        }
    } else if (query[0] == '/') {
        bs.serial_enable = true;
        do_query(&query[1], INTERFACE_USB);
        bs.serial_enable = false;
        return true;
    }

    return false;
}

InputReturn InputStateMachine::check_input(uint8_t ch, bool enable_pt) {
    if (query_pos < MAX_QUERY) {
        query[query_pos++] = ch;
    }

    switch(input_state) {
        case IS_IDLE:
        idle_check:
            if (ch == RN42XV_ESCAPE_CHAR_1) {
                input_state = IS_EVENT1;
            } else if (ch == SOH) {
                input_state = IS_QUERY;
                query_pos = 0;
            } else {
                if (query_pos && enable_pt)
                    send_passthrough();
                query_pos = 0;
            }
            break;
        case IS_EVENT1:
            if (ch == RN42XV_ESCAPE_CHAR_2) {
                input_state = IS_EVENT2;
            } else {
                if (enable_pt)
                    send_passthrough();
                query_pos = 0;
                input_state = IS_IDLE;
                goto idle_check;
            }
            break;
        case IS_EVENT2:
            if (query_pos >= 9 && memcmp(query + 2, "CONNECT", 7) == 0 && ch == 10) {
                input_state = IS_IDLE;
                return IR_CONNECT;
            } else if (query_pos == 12 && memcmp(query + 2, "DISCONNECT", 10) == 0) {
                input_state = IS_IDLE;
                return IR_DISCONNECT;
            } else if (query_pos == 8 && memcmp(query + 2, "REBOOT", 6) == 0) {
                input_state = IS_IDLE;
                return IR_REBOOT;
            } else if (query_pos > 32) {
                if (enable_pt)
                    send_passthrough();
                query_pos = 0;
                input_state = IS_IDLE;
                goto idle_check;
            }
            break;
        case IS_QUERY:
            if (ch == RN42XV_ESCAPE_CHAR_1) {
                input_state = IS_EVENT1;
                query[0] = ch;
                query_pos = 1;
            } else if (ch == SOH) {
                query_pos = 0;
            } else if (ch == ETX) {
                query[query_pos - 1] = 0;
                input_state = IS_IDLE;
                query_pos = 0;
                return IR_QUERY;
            } else if (query_pos >= MAX_QUERY) {
                input_state = IS_IDLE;
                query_pos = 0;
            }
            break;
    }

    return IR_NOACTION;

}

void BTInterface::rpi_inputchar(uint8_t ch) {
#ifdef DEBUG_RPI_PASSTHROUGH
    if (32 < ch && ch < 127) {
        D("R->M: %c", ch);
    } else {
        D("R->M: 0x%x", ch);
    }
#endif
    InputReturn event = rpi_ism.check_input(ch, bt_state == BLUETOOTH_CONNECTED);
    if (event == IR_QUERY) {
        do_query(rpi_ism.query, INTERFACE_RPI);
    }
}

void BTInterface::inputchar(uint8_t ch) {
    //Logger::console("bt_interface: input %d", ch);
#ifdef DEBUG_RPI_PASSTHROUGH
    if (32 < ch && ch < 127) {
        D("P->M: %c", ch);
    } else {
        D("P->M: 0x%x", ch);
    }
#endif

    InputReturn event;

    switch(bt_state) {
        case BLUETOOTH_RESET:
        case BLUETOOTH_WAITRESET:
        case BLUETOOTH_HOLDRESET:
        case BLUETOOTH_POWERON:
            // got char while module powered off? must be noise.
            break;
        case BLUETOOTH_BOOTING:
            // last char in CMD
            if (ch == 'D') {
                D("entered command mode, sending connect");
                //Serial.println("SO,\xFA\xFD");
                Serial.println("C");
                bt_state = BLUETOOTH_CONNECTING;
                bt_ism.input_state = IS_IDLE;
            }
            break;
        case BLUETOOTH_CONNECTING:
            // CONNECT failed
            if (ch == 'f' || ch == 'd') {
                D("connect failed, resetting");
                reset_bluetooth();
            }

            // fallthrough
        case BLUETOOTH_CONNECTED:
        case BLUETOOTH_WAITRETRY:
            event = bt_ism.check_input(ch, bt_state == BLUETOOTH_CONNECTED);
            switch (event) {
                case IR_CONNECT:
                    D("connected");
                    bt_state = BLUETOOTH_CONNECTED;
                    send_event(vehicle_power_on ? EV_PWRON : EV_PWROFF);
                    break;
                case IR_DISCONNECT:
                    D("disconnected, retrying");
                    bt_state = BLUETOOTH_WAITRESET;
                    last_send_millis = millis();
                    break;
                case IR_QUERY:
                    do_query(bt_ism.query, INTERFACE_BT);
                    break;
                default:
                    break;
            }
    }
}

void BTInterface::check_power_on() {
    bool ready = powertrain_enable && cd.odometer != 0;
    if (ready != vehicle_power_on) {
        uint32_t ctime = millis();

        // Don't let it change too quickly - sometimes the power flag bounces back on briefly after turning off.
        if ((ctime - vehicle_power_change_time) < 2000) {
            return;
        }
        vehicle_power_change_time = ctime;
        vehicle_power_on = ready;
        cur_obd_pid = 0;

        if (vehicle_power_on) {
            odometer_po = cd.odometer;
            clock_po = clock;
            clock_po.fw_millis = (millis() - clock.fw_millis);

            wrc_ignoreval = cd.wrc & 0x3FF;

            reset();
            send_event(EV_PWRON);
            D("key on %d-%d-%d %d:%d:%d + %d", clock.year, clock.month, clock.day, clock.hour, clock.min, clock.sec, clock_po.fw_millis);
        } else {
            send_event(EV_PWROFF);
            bt_lcd.enable_send = SEND_DISABLE;
            //rpi_lcd.enable_send = SEND_DISABLE;
            D("key off");
        }

    }
}

void BTInterface::send_obd2_query(int whichmod, int pid_to_send) {
    CAN_FRAME sframe;

    sframe.extended = false;
    if (whichmod == -1) {
        D("send query: (all) %x", pid_to_send);
        sframe.id = 0x7DF;
    } else {
        sframe.id = 0x7E0 + whichmod;
        D("send query: (%x) %x", sframe.id, pid_to_send);
    }

    sframe.rtr = 0;
    sframe.length = 8;
    sframe.data.bytes[0] = 2;
    sframe.data.bytes[1] = 1;
    sframe.data.bytes[2] = pid_to_send & 0xFF;
    sframe.data.bytes[3] = 0;
    sframe.data.bytes[4] = 0;
    sframe.data.bytes[5] = 0;
    sframe.data.bytes[6] = 0;
    sframe.data.bytes[7] = 0;

    if (pid_to_send == PID_TROUBLECODE_QUERY) {
        sframe.data.bytes[0] = 1;
        sframe.data.bytes[1] = 3;
        sframe.data.bytes[2] = 0;
    } else if (pid_to_send > 0xFF) {
        sframe.data.bytes[0] = 3;
        sframe.data.bytes[1] = 0x22;
        sframe.data.bytes[2] = (pid_to_send >> 8) & 0xFF;
        sframe.data.bytes[3] = pid_to_send & 0xFF;
    }

    Can0.sendFrame(sframe);
}

void CommandButtonManager::send(int button) {
    CAN_FRAME sframe;
    sframe.extended = true;
    sframe.data.value = 0;
    sframe.rtr = 0;
    build_frame(sframe, button);

    // shitty SWCAN library does SPIO in interrupt handler, so if a
    // frame is received while we are in the middle of transmitting a
    // frame, it fucks everything up

    noInterrupts();
    SWCAN.sendFrame(sframe);
    interrupts();

    last_button_send = millis();

    // for the 1 in 4 billion chance that we send the button just as millis() overflows
    if (last_button_send == 0) last_button_send = 1;
}

void CommandButtonManager::check_release(uint32_t msnow) {
    if (last_button_send && (msnow - last_button_send) > 50) {
        send(-1);
        last_button_send = 0;
    }
}

void WindowRollTimer::send(uint8_t cmd, uint8_t mask, uint16_t duration) {
    D("WindowRollTimer::send(%d, %d, %d)", cmd, mask, duration);

    bti.diag[0].diag_enable(DU_WINDOW_ROLL, duration != 0);
    bti.diag[0].enqueue_diag(0x3B, 0xFF, mask & 1 ? cmd : 0, mask & 2 ? cmd : 0, mask & 4 ? cmd : 0, mask & 8 ? cmd : 0);

    last_send = millis();
    this->duration = duration;
}

void WindowRollTimer::check_release(uint32_t msnow) {
    if (duration && (msnow - last_send) > duration) {
        send(0, 0, 0);
    }
}

void BTInterface::reset_bluetooth() {
    digitalWrite(XBEE_RST, LOW);
    last_send_millis = millis();
    bt_state = BLUETOOTH_HOLDRESET;
}


void BTInterface::loop() {
    static uint32_t button1bouncecount = 0;
    static uint32_t button2bouncecount = 0;

    if (!xmit_enable()) {
        asm("wfi");
    }

    uint32_t msnow = millis();
    climate_button.check_release(msnow);
    radio_button.check_release(msnow);
    wrt.check_release(msnow);

    int buttonState = digitalRead(Button2);

    if (buttonState == LOW) {
        if (button2bouncecount < 10) {
            if (++button2bouncecount == 10) {
                xmit_override = !xmit_override;
                D("button pressed, setting xmit_override=%d", xmit_override);
            }
        }
    } else {
        button2bouncecount = 0;
    }

    buttonState = digitalRead(Button1);

    if (buttonState == LOW) {
        if (button1bouncecount < 10) {
            if (++button1bouncecount == 10) {
                bt_power_override = !bt_power_override;
                D("button pressed, setting bt_power_override=%d", bt_power_override);
            }
        }
    } else {
        button1bouncecount = 0;
    }

    int dc_profile_index = 0;
    if (vehicle_power_on) {
        dc_profile_index = 1;
        if (cd.gear != 0) {
            dc_profile_index = 2;
        }
    }
    if (dc_profile_index != last_dc_profile_index) {
        int dc_command = (settings.dashcamProfile >> (dc_profile_index * 2)) & 0x3;
        if (dc_command == 2) {
            set_dashcam_command(false);
        } else if (dc_command == 3) {
            set_dashcam_command(true);
        }
        last_dc_profile_index = dc_profile_index;
    }

    if (bus_active) {
        if ((msnow - last_frame_recv) > 500) {
            bus_active = false;
            send_event(EV_BUS_INACTIVE);
        }
    }

#define CHECK_KEY_ON() if (!xmit_enable()) { last_send_millis = msnow; bt_state = BLUETOOTH_WAITRESET; D("waitreset");} else (void)0
    switch(bt_state) {
        case BLUETOOTH_RESET:
            digitalWrite(RGB_GREEN, HIGH);
            if (bt_power_override) {
                D("enabling bluetooth power");
                bt_state = BLUETOOTH_POWERON;
                digitalWrite(XBEE_RST, HIGH);
            } else if (xmit_enable()) {
                D("enabling bluetooth");
                digitalWrite(XBEE_RST, HIGH);
                bt_state = BLUETOOTH_BOOTING;
                last_send_millis = 0;
                boot_command_attempt_count = 0;
            }

            break;

        case BLUETOOTH_POWERON:
            if (!bt_power_override) {
                D("disabling bluetooth power");
                bt_state = BLUETOOTH_WAITRESET;
                digitalWrite(XBEE_RST, HIGH);
            }
            break;
        case BLUETOOTH_HOLDRESET:
            digitalWrite(RGB_GREEN, HIGH);
            if ((msnow - last_send_millis) > 1000) {
                D("reset hold complete");
                bt_state = BLUETOOTH_RESET;
            }
            break;

        case BLUETOOTH_WAITRESET:
            digitalWrite(RGB_GREEN, HIGH);
            if ((msnow - last_send_millis) > 1500) {
                D("disabling bluetooth");
                reset_bluetooth();
            }
            break;

        case BLUETOOTH_BOOTING:
            CHECK_KEY_ON();
            if ((msnow - last_send_millis) > 100) {
                if (boot_command_attempt_count++ == 15) {
                    D("boot timeout, resetting");
                    reset_bluetooth();
                } else {
                    last_send_millis = msnow;
                    Serial.print("$$$");
                }
            }
            digitalWrite(RGB_GREEN, (msnow % 250) > 125 ? LOW : HIGH);
            break;

        case BLUETOOTH_CONNECTING:
            CHECK_KEY_ON();
            digitalWrite(RGB_GREEN, (msnow % 1500) > 750 ? LOW : HIGH);
            if ((msnow - last_send_millis) > 6000) {
                D("connect timeout, resetting");
                reset_bluetooth();
            }
            break;

        case BLUETOOTH_CONNECTED:
            CHECK_KEY_ON();
            digitalWrite(RGB_GREEN, LOW);
            break;

        case BLUETOOTH_WAITRETRY:
            CHECK_KEY_ON();
            digitalWrite(RGB_GREEN, (msnow % 150) > 75 ? LOW : HIGH);
            if ((msnow - last_send_millis) > 1500) {
                D("reconnecting");
                Serial.println('C');
                bt_state = BLUETOOTH_CONNECTING;
            }
            break;

    }

    //uint32_t obd2_query_interval = vehicle_power_on ? 25 : 0;

    if ((msnow - last_periodic_millis) > 8) {
        last_periodic_millis += 8;
        if ((msnow - last_periodic_millis) > 4) {
            last_periodic_millis = msnow - 4;
        }
        periodic_cycle++;
        if ((periodic_cycle & 1) == 0) {
            if (vehicle_power_on) {
                uint32_t encoded_pid = (vehicle_power_on ? obd2_query_sequence : obd2_query_sequence_low)[cur_obd_pid];
                if (encoded_pid == 0) {
                    encoded_pid = obd2_query_sequence[0];
                    cur_obd_pid = 0;
                }

                if (encoded_pid != PID_NOTHING_TO_SEND) {
                    uint16_t pid = encoded_pid & 0xFFFF;
                    int whichmod = ((encoded_pid >> 16) & 15) - 1;
                    send_obd2_query(whichmod, pid);
                }

                cur_obd_pid++;
            }
        }
    }

    for (int i = 0; i < 3; i++) {
        diag[i].check_send(msnow);
    }

    if (bt_state == BLUETOOTH_CONNECTED && bt_lcd.should_send(msnow, vehicle_power_on ? SEND_INTERVAL : SEND_INTERVAL_LOW)) {
        build_data_frame(bt_lcd);
        send_frame_bt();
    }
    if (rpi_lcd.should_send(msnow, vehicle_power_on ? SEND_INTERVAL : SEND_INTERVAL_LOW)) {
        build_data_frame(rpi_lcd);
        send_frame_rpi();
    }
}

const char DTC_CC[] = "PCBU";
const char HEX_CC[] = "0123456789ABCDEF";

static void convert_dtc(char dest[6], int dtc) {
    dest[0] = DTC_CC[(dtc >> 14) & 3];
    dest[1] = HEX_CC[(dtc >> 12) & 3];
    dest[2] = HEX_CC[(dtc >> 8) & 15];
    dest[3] = HEX_CC[(dtc >> 4) & 15];
    dest[4] = HEX_CC[(dtc) & 15];
    dest[5] = 0;
}

void BTInterface::handle_obd2_reply(CAN_FRAME &frame) {
    uint16_t pid = 0, vab;
    uint8_t tcc, va, vb, vc, vd;
    uint16_t dtc1, dtc2;
    uint8_t responding_mod = frame.id & 7;
    int count = 0;

    switch(frame.data.bytes[1]) {
        // Standard PIDs
        case 0x41:
            count = GET8(frame, 0) - 1;
            pid = GET8(frame, 2);
            va = GET8(frame, 3);
            vb = GET8(frame, 4);
            break;
        case 0x62:
            count = GET8(frame, 0) - 2;
            pid = GET16(frame, 2);
            va = GET8(frame, 4);
            vb = GET8(frame, 5);
            vc = GET8(frame, 6);
            vd = GET8(frame, 7);
            break;

        // Trouble code query reply
        case 0x43:
            count = GET8(frame, 0);
            tcc = GET8(frame, 2);
            dtc1 = count >= 4 ? GET16(frame, 3) : 0;
            dtc2 = count >= 6 ? GET16(frame, 5) : 0;
            for (int i = 0; i < INTERFACE_MAX; i++) {
                obd_query& obd(obd_queries[i]);
                bool requested = PID_TROUBLECODE_QUERY == obd.pid && (millis() - obd.send_time) < 250 && (obd.whichmod == -1 || responding_mod == obd.whichmod);
                bool important = tcc != 0 && (dtc1 != 0 || dtc2 != 0);
                if (requested || important) {
                    if (i == INTERFACE_USB) {
                        char dtc_text[6];
                        if (requested || SysSettings.btDebugEnable) {
                            if (dtc1) {
                                convert_dtc(dtc_text, dtc1);
                                Logger::console("TC: m=%d (%d) %s", responding_mod, tcc, dtc_text);
                            }
                            if (dtc2) {
                                convert_dtc(dtc_text, dtc2);
                                Logger::console("TC: m=%d (%d) %s", responding_mod, tcc, dtc_text);
                            }
                        }
                        if (requested && !(dtc1 || dtc2)) {
                            Logger::console("TC: m=%d (%d)", responding_mod, tcc);
                        }
                    } else {
                        begin_frame(FT_TCODE);
                        WBITS(3, responding_mod);
                        WBITS(8, tcc);
                        WBITS(16, dtc1);
                        WBITS(16, dtc2);
                        bs.close_frame();
                        send_frame((Interface)i);
                    }
                }
            }
            break;
        default:
            if (frame.id == 0x7E9) {
                diag[1].send_next_diag_frame(millis());
            } else if (frame.id == 0x7EC) {
                diag[2].send_next_diag_frame(millis());
            }

            if (log_unusual_replies) {
                char text[32];
                int count = 1 + GET8(frame, 0);
                int pos = 0;
                if (count > 8) count = 8;
                for (int i = 0; i < count; i++) {
                    unsigned char v = GET8(frame, i);
                    text[pos++] = HEX_CC[(v >> 4) & 0xF];
                    text[pos++] = HEX_CC[v & 0xF];
                    text[pos++] = ' ';
                }
                text[pos] = 0;
                Logger::console("OBD frame: %x %s", frame.id, text);
            }
    }

    if (pid != 0) {
        vab = (va << 8) | vb;
        for (int i = 0; i < INTERFACE_MAX; i++) {
            obd_query& obd(obd_queries[i]);

            if (pid == obd.pid && (millis() - obd.send_time) < 250 && (obd.whichmod == -1 || responding_mod == obd.whichmod)) {
                int mod_bit = 1 << responding_mod;
                if (!(obd.responded_mods & mod_bit)) {
                    obd.responded_mods |= mod_bit;
                    if (i == INTERFACE_USB) {
                        if (count == 1) {
                            Logger::console("OBD(%d): %d %x v=%d", count, responding_mod, pid, va);
                        } else if (count == 2) {
                            Logger::console("OBD(%d): %d %x v=%d", count, responding_mod, pid, vab);
                        } else {
                            Logger::console("OBD(%d): %d %x a=%d ab=%d (%x %x %x %x)", count, responding_mod, pid, va, vab, va, vb, vc, vd);
                        }
                    } else {
                        begin_frame(FT_OBD);
                        WBITS(3, responding_mod);
                        WBITS(16, pid);
                        WBITS(16, vab);
                        WBITS(8, vc);
                        WBITS(8, vd);
                        bs.close_frame();
                        send_frame((Interface)i);
                    }
                }
            }
        }

        switch (pid) {
            #include "obd2_switch.h"
        }
    }
}


void BTInterface::gotframe(CAN_FRAME &frame, int busnum) {
    uint32_t now = millis();

    switch(frame.id) {
        case 0x0c7: {
            uint16_t val = GET16(frame, 0) & 0x3FF;
            if (val != wrc_ignoreval) {
                wrc_ignoreval = 0xFFFF;
                cd.wrc = rollover_bits<10>(cd.wrc, val);
            }
        } break;
        case 0x0c9: {
            powertrain_enable = (frame.data.bytes[0] & 0xC0) != 0;
            check_power_on();
            cd.engine_rpm = GET16(frame, 1) >> 2;
            if (fake_engine) cd.engine_rpm = 800 + (12 * cd.accel);

            last_frame_recv = now;
            if (!bus_active) {
                bus_active = true;
                send_event(EV_BUS_ACTIVE);
            }
        } break;
        case 0xBC: {
            cd.clutch_state = GET8(frame, 2);
            cd.mga_rpm = GET16(frame, 4);
            cd.mgb_rpm = GET16(frame, 6);
        } break;

        case 0x120: {
            cd.odometer = GET32(frame, 0);
            if (powertrain_enable) {
                check_power_on();
            }
        } break;
        case 0x160: {
            bool key = GET8(frame, 0) != 0;
            if (key != key_on) {
                key_on = key;
                send_event(key ? EV_KEYON : EV_KEYOFF);
            }
        } break;

        case 0x102CA040: {
            int ngear = frame.data.bytes[6] & 7;
            if (ngear > 0 && ngear < 6) {
                cd.gear = ngear - 1;
            }
        } break;

        case 0x3e9:
            cd.speed = GET16(frame, 0);
            cd.wrc1 = GET16(frame, 2) & 0x1FFF;
            cd.wrc2 = GET16(frame, 6) & 0x1FFF;
            break;

        case 0x1a1:
            cd.accel = GET8(frame, 6);
            break;

        case 0xd1:
            cd.brake = GET8(frame, 4);
            break;

        case 0x1e5:
            cd.steer = GET16(frame, 1);
            break;

        case 0x140:
            cd.scflags = GET24(frame, 0);
            break;

        case 0x32a:
            cd.lat = GET32(frame, 0);
            cd.lon = GET32(frame, 4);
        break;

        case 0x3f9:
            cd.fuel_ctr = rollover_bits<13>(cd.fuel_ctr, fake_engine ? ((cd.fuel_ctr + 10 + cd.accel) & 0x1FFF) : GET16(frame, 1));
            break;

        case 0x3d3: {
            uint16_t rawspeed = GET16(frame, 2);
            if (rawspeed & 0x8000) {
                uint16_t active = GET8(frame, 4);
                cd.ccspeed = rawspeed & 0x0FFF;
                if (active & 0x80) {
                    cd.ccspeed |= 0x1000;
                }
            }
        } break;

        case 0x0C414040: {
            switch (GET8(frame, 1)) {
                case 0x05: send_event(EV_LOCK); break;
                case 0x04: send_event(EV_UNLOCK); break;
            }
        } break;

        case 0x102a8097: {
            int year = GET8(frame, 0);
            int month = GET8(frame, 1);
            if (year != 0 && month != 0) {
                clock.fw_millis = millis();
                clock.year = year;
                clock.month = month;
                clock.day = GET8(frame, 2);
                clock.hour = GET8(frame, 3);
                clock.min = GET8(frame, 4);
                clock.sec = GET8(frame, 5);
            }
        } break;

        case 0x102D0040: {
            cd.oil_life = GET8(frame, 4);
        } break;

        case 0x103D4040: {
            cd.tire_ft_lf = GET8(frame, 2);
            cd.tire_rr_lf = GET8(frame, 3);
            cd.tire_ft_rt = GET8(frame, 4);
            cd.tire_rr_rt = GET8(frame, 5);
        } break;

        case 0x10758040: {
            cd.ccbtn = GET8(frame, 0);
            int btn_mask = cd.ccbtn & 0xE;
            if (lastccbtn != btn_mask) {
                lastccbtn = btn_mask;
                switch(btn_mask) {
                    case 4: send_event(EV_CCUP); break;
                    case 6: send_event(EV_CCDN); break;
                    case 10: send_event(EV_CCPWR); break;
                    case 12: send_event(EV_CCCANCEL); break;
                }
            }

        } break;

        case 0x10424040: {
            cd.air_temp1 = GET8(frame, 1);
            cd.air_temp2 = GET8(frame, 2);
        } break;

        case 0x10438040: {
            cd.radiobtn = GET8(frame, 0);
            int btn_mask = cd.radiobtn & 0xF;
            if (lastradiobtn != btn_mask) {
                lastradiobtn = btn_mask;
                switch(btn_mask) {
                    case 1: send_event(EV_VOLUP); break;
                    case 2: send_event(EV_VOLDN); break;
                    case 3: send_event(EV_TRACKUP); break;
                    case 4: send_event(EV_TRACKDN); break;
                    case 5: send_event(EV_SRC); break;
                    case 6: send_event(EV_VOICE); break;
                    case 7: send_event(EV_MUTE); break;
                }
            }

        } break;
        case 0x10254040: {
            switch(GET8(frame, 0)) {
                case 0: cd.drive_mode = 0; break;
                case 1: cd.drive_mode = 1; break;
                case 4: cd.drive_mode = 2; break;
                case 16: cd.drive_mode = 3; break;
            }
        } break;

        case 0x10814099: {
            cd.fanspeed = GET8(frame, 1);
        } break;

        case 0x10B02099: {
            int which = GET8(frame, 3);
            int val = GET8(frame, 7);
            switch (which) {
                case 0x01: cd.vent = val; break;
                case 0x02: cd.select_fanspeed = val; break;
                case 0x03: cd.select_temp = val; break;
                case 0x0D: cd.recirc = val; break;
                case 0x14: cd.climate_mode = val; break;
                case 0x15: cd.climate_power = val; break;
                case 0x2F: cd.heat_ac = val; break;
            }
        } break;

        case 0x10380099: {
            cd.rear_defrost = GET8(frame, 0) & 1;
        } break;

        case 0x641:
            if (log_unusual_replies) {
                char text[32];
                int count = 1 + GET8(frame, 0);
                int pos = 0;
                for (int i = 0; i < 8; i++) {
                    unsigned char v = GET8(frame, i);
                    text[pos++] = HEX_CC[(v >> 4) & 0xF];
                    text[pos++] = HEX_CC[v & 0xF];
                    text[pos++] = ' ';
                }
                text[pos] = 0;
                Logger::console("Reply frame: %x %s", frame.id, text);
            }

            diag[0].send_next_diag_frame(millis());
            break;
        case 0x7e8:
        case 0x7e9:
        case 0x7ea:
        case 0x7eb:
        case 0x7ec:
        case 0x7ed:
        case 0x7ee:
        case 0x7ef:
            handle_obd2_reply(frame);
            break;

    }
}
