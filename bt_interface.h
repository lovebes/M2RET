
extern void bt_setup();
extern void bt_loop();
extern void bt_sendframe(CAN_FRAME &frame, int busnum);
extern void bt_inputchar(uint8_t ch);
extern void bt_u3inputchar(uint8_t ch);
extern bool bt_command(const char* cmd);
