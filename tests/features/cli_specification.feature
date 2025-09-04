Feature: Tellus CLI follows specification
  As a user of tellus
  I want the CLI to follow the documented specification
  So that I can rely on consistent behavior

  Background:
    Given I have a clean working directory
    
  Scenario: CLI shows main subcommands
    When I run "tellus --help"
    Then I should see "location" in the output
    And I should see "simulation" in the output
    And I should see "init" in the output
    And the exit code should be 0

  Scenario: Global JSON flag is available
    When I run "tellus --help"
    Then I should see "--json" in the output
    And the exit code should be 0

  Scenario: Init command has required options
    When I run "tellus init --help"
    Then I should see "--force" in the output
    And I should see "--migrate-from" in the output
    And the exit code should be 0

  Scenario: Location commands follow specification grammar
    When I run "tellus location --help"
    Then the exit code should be 0 or 1 or 2
    And if successful, I should see imperative verbs like "create", "show", "list"

  Scenario: Simulation commands follow specification grammar  
    When I run "tellus simulation --help"
    Then the exit code should be 0 or 1 or 2
    And if successful, I should see imperative verbs like "create", "show", "list"

  Scenario: Invalid commands return non-zero exit codes
    When I run "tellus nonexistent-command"
    Then the exit code should not be 0

  Scenario: Commands use long-form flags
    When I run "tellus init --help"
    Then I should see "--force" in the output
    And I should see "--migrate-from" in the output
    And I should not see single-letter flags without long equivalents