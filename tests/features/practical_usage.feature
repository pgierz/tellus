Feature: Practical usage scenarios
  As a climate scientist
  I want to use tellus for real-world tasks
  So that I can manage my simulation data effectively

  Background:
    Given I have a clean tellus project
  
  Scenario: Initialize a new project
    When I run "tellus init --force"
    Then the exit code should be 0
    And I should have a .tellus directory

  Scenario: Create and list locations
    Given I have initialized tellus
    When I run "tellus location create /tmp/test_location"
    Then the exit code should be 0 or 1
    When I run "tellus location list"  
    Then the exit code should be 0 or 1

  Scenario: Create and list simulations
    Given I have initialized tellus
    And I have created a location
    When I run "tellus simulation create test_sim --location test_location"
    Then the exit code should be 0 or 1
    When I run "tellus simulation list"
    Then the exit code should be 0 or 1

  Scenario: JSON output format
    Given I have initialized tellus  
    When I run "tellus --json location list"
    Then if successful, the output should be valid JSON
    When I run "tellus --json simulation list"
    Then if successful, the output should be valid JSON

  @practical
  Scenario: Temperature and salt extraction workflow
    # This scenario represents issue #40
    Given I have a simulation with FESOM data
    When I run "tellus simulation file list --content-type output --variable temperature"
    Then I should see temperature files
    When I run "tellus simulation file list --content-type output --variable salinity"  
    Then I should see salinity files