import asyncio

metrics = {}


class ClientServerProtocol(asyncio.Protocol):

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        try:
            response = self.process_data(data.decode('utf-8').strip('\n'))
            self.transport.write(response.encode('utf-8'))
        except:
            self.transport.write('error'.encode('utf-8'))

    @staticmethod
    def put(key, value, timestamp):
        if key == '*':
            return 'error\nwrong command\n\n'
        if key not in metrics:
            metrics[key] = []
        if (timestamp, value) not in metrics[key]:
            metrics[key].append((timestamp, value))
            metrics[key].sort(key=lambda x: x[0])
        return 'ok\n\n'

    @staticmethod
    def get(key):
        s = ''
        if key != '*':
            if key in metrics:
                for values in metrics[key]:
                    s += f'{key} {values[1]} {values[0]}\n'
        elif key == '*':
            for key, values in metrics.items():
                for value in values:
                    s += f'{key} {value[1]} {value[0]}\n'
        answer = 'ok\n'
        answer += s
        return answer + '\n'

    def process_data(self, data):
        try:
            components = data.split(' ')
        except:
            return 'error with data separation\n\n'
        command = components[0]
        if command == 'put':
            return self.put(components[1], components[2], components[3])
        elif command == 'get':
            return self.get(components[1])
        else:
            return 'error\nwrong command\n\n'


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(
        ClientServerProtocol,
        host, port
    )
    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == "__main__":
    run_server("127.0.0.1", 8888)
