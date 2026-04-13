import os
import cmd
import sys
from mango import loadsheet_to_bacnet_scan, loadsheet_to_building_config, combine_bacnet_scans, augment_site_model
from db_api import execute_api_calls, split_config, export_building_config

class Mapper(cmd.Cmd):
    def __init__(self):
        super(Mapper, self).__init__()
        self._flush_input()
        self._clear()
        
        self.intro = "" 
        self.prompt = '>>> '

    def _clear(self):
        os.system('cls' if os.name == 'nt' else 'clear')

    def _flush_input(self):
        """ Clears the terminal input buffer so old keystrokes don't leak in """
        try:
            import termios
            termios.tcflush(sys.stdin, termios.TCIFLUSH)
        except ImportError:
            # Fallback for non-linux systems if needed
            pass

    def preloop(self):
        """ Runs once at the very start """
        print("================================================")
        print("Welcome to DB Engineering Onboarding Utilities.")
        print("================================================")
        
        self.do_menu(None)

    def postcmd(self, stop, line):
        """ Runs after every tool finishes """
        if not stop:
            self.do_menu(None)
        return stop

    def do_menu(self, arg):
        """Show the numbered selection menu"""
        print('\nWhat would you like to do?')
        print('1: Populate bacnet-scan from loadsheet')
        print('2: Create a building config from loadsheet')
        print('3: Export a new building config')
        print('4: Split a building config')
        print('5: Run an onboarding operation')
        print('6: Combine multiple bacnet-scans into one')
        print('7: Augment Site Model')
        print('q: quit\n')

    def do_1(self, arg):
        """Populate bacnet-scan from loadsheet"""
        print("""The following inputs will be needed:
                loadsheet:\t\t[required]\tloadsheet, format: [.xlsx]
                bacnet-scan:\t\t[required]\toutput of a bacnet-scan tool, format: [.xlsx]
                mango config file:\t[optional]\tneeded for identifying existing proxy_id
                """)
        try:
            loadsheet_to_bacnet_scan.main()
        except Exception as e:
            print(f"[ERROR]: Unable to create building config: {e}")

    def do_2(self, arg):
        """Create a building config from loadsheet"""
        print("""The following inputs will be needed:
                loadsheet:\t\t[required]\tloadsheet, format: [.xlsx]
                site model:\t\t[required]\ta path to the clone of the site from Cloud Source Repo
                device discovery:\t[required]\tquery result from https://plx.corp.google.com/scripts2/script_5e._f704e7_7fe1_486b_8d3c_f3a20190d94e [.csv]
                output path:\t\t[required]
                """)
        try:
            loadsheet_to_building_config.main()
        except Exception as e:
            print(f"[ERROR]: Unable to create building config: {e}")

    def do_3(self, arg):
        """Export a new building config"""
        try:
            export_building_config.main()
        except Exception as e:
            print(f"[ERROR]: Unable to export building config: {e}")

    def do_4(self, arg):
        """Split a building config"""
        try:
            split_config.main()
        except Exception as e:
            print(f"[ERROR]: Unable to split config: {e}")

    def do_5(self, arg):
        """Export a new building config"""
        try:
            execute_api_calls.main() 
        except Exception as e:
            print(f"[ERROR]: Unable to run onboarding operation: {e}")

    def do_6(self, arg):
        """Combine multiple bacnet-scans into one"""
        try:
            combine_bacnet_scans.main() 
        except Exception as e:
            print(f"[ERROR]: Unable to combine bacnet-scans: {e}")

    def do_7(self, arg):
        """Create a building config from loadsheet"""
        print("""The following inputs will be needed:
                loadsheet:\t\t[required]\tloadsheet, format: [.xlsx]
                device discovery:\t[required]\tquery result from https://plx.corp.google.com/scripts2/script_5e._f704e7_7fe1_486b_8d3c_f3a20190d94e [.csv]
                carson config:\t\t[required]\tfresh Building Config export from DB API [.yaml]
                site model:\t\t[required]\ta path to the clone of the site from Cloud Source Repo
                """)
        # try:
        augment_site_model.main()
        # except Exception as e:
        #     print(f"[ERROR]: Unable to augment site model: {e}")

    def do_q(self, arg):
        """Quits Program"""
        print("Goodbye!")
        return True

    def do_quit(self, arg):
        """Quits Program"""
        return self.do_q(None)

    def default(self, line):
        """Handle numeric input if user just types the number"""
        if line == '1': return self.do_1(None)
        if line == '2': return self.do_2(None)
        if line == '3': return self.do_3(None)
        if line == '4': return self.do_4(None)
        if line == '5': return self.do_5(None)
        if line == '6': return self.do_6(None)
        if line == '6': return self.do_7(None)
        if line == 'q': return self.do_q(None)
        print(f"*** Unknown syntax: {line}. Please choose 1-4 or q.")

def main():
    try:
        con = Mapper()
        con.cmdloop()
    except KeyboardInterrupt:
        print("\n\n[Interrupted] Closing utilities...")
        sys.exit(0)

if __name__ == '__main__':
    main()