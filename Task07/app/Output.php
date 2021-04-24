<?php

class Output
{
    public function echoReceptions(array $receptions, array $fieldNames): void
    {
        if (count($receptions) < 1) {
            $this->echoMessage('Receptions not found');
            return;
        }

        $this->echoHeader($fieldNames);

        foreach ($receptions as $reception) {
            $this->echoRow([
                $reception->id,
                $reception->firstName,
                $reception->lastName,
                $reception->patronymic ?? 'none',
                $reception->serviceName,
                $reception->status,
                $reception->endedAt ?? 'Pending',
                $reception->price . 'RUB',
            ], '  |  ');
            $this->echoGentleLine(104);
        }
    }

    public function echoError(string $message): void
    {
        $this->echoMessage("Error: $message");
    }

    private function echoHeader(array $fieldNames): void
    {
        $this->echoStrongLine(104);
        $this->echoRow($fieldNames, '  |  ');
        $this->echoStrongLine(104);
    }

    private function echoStrongLine(int $length): void
    {
        $this->echoMessage(str_repeat('=', $length) . PHP_EOL);
    }

    private function echoGentleLine(int $length): void
    {
        $this->echoMessage(str_repeat('-', $length) . PHP_EOL);
    }

    private function echoRow(array $parameters, string $glue = ' '): void
    {
        $this->echoMessage('|  ' . implode($glue, $parameters) . '  |' . PHP_EOL);
    }

    private function echoMessage(string $message): void
    {
        echo $message;
    }
}
