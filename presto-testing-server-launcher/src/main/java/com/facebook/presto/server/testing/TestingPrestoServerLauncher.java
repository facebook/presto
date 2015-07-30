/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server.testing;

import com.facebook.presto.server.testing.TestingPrestoServerLauncherOptions.Catalog;
import com.facebook.presto.spi.Plugin;
import io.airlift.command.Command;
import io.airlift.command.Help;
import io.airlift.command.HelpOption;
import io.airlift.command.model.CommandMetadata;

import javax.inject.Inject;

import static io.airlift.command.SingleCommand.singleCommand;

@Command(name = "testing_presto_server", description = "Testing Presto Server Launcher")
public class TestingPrestoServerLauncher
{
    @Inject CommandMetadata commandMetadata;

    @Inject
    public HelpOption helpOption;

    @Inject
    TestingPrestoServerLauncherOptions options = new TestingPrestoServerLauncherOptions();

    private static void waitForInterruption()
    {
        try {
            Thread.currentThread().join();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void run()
            throws Exception
    {
        TestingPrestoServer server = new TestingPrestoServer();
        try {
            for (String pluginClass : options.getPluginClassNames()) {
                Plugin plugin = (Plugin) Class.forName(pluginClass).newInstance();
                server.installPlugin(plugin);
            }

            for (Catalog catalog : options.getCatalogs()) {
                server.createCatalog(catalog.getCatalogName(), catalog.getConnectorName());
            }

            System.out.println(server.getAddress());
            waitForInterruption();
        }
        finally {
            server.close();
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        TestingPrestoServerLauncher launcher = singleCommand(TestingPrestoServerLauncher.class).parse(args);
        if (launcher.helpOption.showHelpIfRequested()) {
            return;
        }
        try {
            launcher.validateOptions();
        }
        catch (IllegalStateException e) {
            System.out.println("ERROR: " + e.getMessage());
            System.out.println();
            Help.help(launcher.commandMetadata);
            return;
        }
        launcher.run();
    }

    private void validateOptions()
    {
        options.validate();
    }
}
